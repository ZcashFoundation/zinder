//! Private compact-blocks compatibility composition.
//!
//! This module owns the canonical-secondary lifecycle that publishes the
//! reader [`zinder_compat_lightwalletd::CompactBlocksGrpcAdapter`] serves. No
//! compact-only topology type crosses the binary boundary.
//!
//! Both generations open on one block cache that the publisher owns, so the
//! configured cache size bounds the process, and a generation opened on a
//! refresh finds the blocks the previous one loaded.

use std::{num::NonZeroU32, path::PathBuf, sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use zinder_compat_lightwalletd::CanonicalReaderSlot;
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
    CanonicalFenceConvergencePolicy, CanonicalFenceProbe, CanonicalFenceRelation,
    ComposedReadiness, Readiness, ReadinessCause, ReadinessState, WriterAttestation,
    classify_canonical_writer_fence, connect_zinder_grpc, converge_on_canonical_writer_fence,
    expected_writer_fence, refresh_within_stale_deadline,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload, RawBlobRetention,
    ResourceGaugeThrottle, RocksDbCanonicalSecondary, RocksDbOpenResources, RocksDbResourceBudget,
    SharedBlockCache, StoreRole, record_instance_memory_gauges, record_shared_block_cache_gauges,
};

const GENERATION_COUNT: usize = 2;
const CONVERGENCE_RETRY_DELAY_CAP: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactBlockServingError {
    #[error("compact-block storage failed: {0}")]
    Storage(#[from] CanonicalStoreError),
    #[error("compact-block generation could not be admitted: {0}")]
    Admission(#[from] StorageReadError),
    #[error("compact-block writer status failed: {0}")]
    WriterStatus(String),
    #[error("compact-block runtime failed: {0}")]
    Runtime(String),
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

#[derive(Debug, thiserror::Error)]
enum ConvergenceFailure {
    #[error("writer status transport failed: {0}")]
    WriterStatusTransport(String),
    #[error("writer is ahead of the candidate secondary")]
    WriterAhead,
    #[error("writer fence diverged from the candidate secondary")]
    WriterFenceDiverged,
    #[error(
        "the canonical writer has not admitted a network-upgrade schedule for the candidate secondary to adopt"
    )]
    WriterScheduleNotAdmitted,
    #[error("candidate storage failed: {0}")]
    Storage(CompactBlockServingError),
}

/// Readiness of compact-blocks serving, which follows the published canonical
/// generation alone: no compact-blocks method calls the node.
#[derive(Clone)]
pub(crate) struct CompactServingReadiness {
    composed: ComposedReadiness,
}

impl CompactServingReadiness {
    pub(crate) fn new(runtime: Readiness) -> Self {
        Self {
            composed: ComposedReadiness::new(runtime, &[]),
        }
    }

    pub(crate) fn runtime(&self) -> Readiness {
        self.composed.runtime_readiness()
    }

    pub(crate) fn publish_storage(&self, storage: ReadinessState) {
        self.composed.publish_admission(storage);
    }

    /// Records the writer's commit time of the chain epoch `reader` serves.
    fn publish_served_epoch(&self, reader: &RocksDbCanonicalReader) {
        self.composed
            .runtime_readiness()
            .set_served_epoch_committed_at(reader.chain_epoch().created_at);
    }

    pub(crate) fn publish_shutting_down(&self) {
        self.composed.publish_shutting_down();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompactBlockServingConfig {
    pub(crate) canonical_primary_path: PathBuf,
    pub(crate) canonical_secondary_root: PathBuf,
    pub(crate) network: Network,
    pub(crate) raw_blob_retention: RawBlobRetention,
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
    /// The block cache both generations open on, sized by the configured
    /// budget. It outlives each generation, so a cold open on a refresh
    /// starts with the blocks the replaced generation loaded.
    block_cache: SharedBlockCache,
    /// Gates the memory sample of the open generations, whose table-cache
    /// lookups are too costly to repeat on every refresh.
    instance_memory_throttle: ResourceGaugeThrottle,
    /// Last attestation of the active reader by a following writer with a
    /// confirmed tip, and the halt the writer last reported, which
    /// writer-status failures republish.
    writer_attestation: WriterAttestation,
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
        let block_cache = SharedBlockCache::new(config.resource_budget.block_cache_bytes);
        let mut writer_attestation =
            WriterAttestation::new(config.network, config.staleness_ceiling);
        let (candidate, attesting_status) = converge_candidate(
            &mut writer_status,
            &config,
            CandidateGeneration {
                block_cache: &block_cache,
                generation: 0,
            },
            ServedReader {
                attestation: &mut writer_attestation,
                readiness: &readiness,
                visible_height: None,
            },
        )
        .await
        .map_err(|failure| match failure {
            ConvergenceFailure::Storage(error) => error,
            failure @ (ConvergenceFailure::WriterStatusTransport(_)
            | ConvergenceFailure::WriterAhead
            | ConvergenceFailure::WriterFenceDiverged
            | ConvergenceFailure::WriterScheduleNotAdmitted) => {
                CompactBlockServingError::Runtime(failure.to_string())
            }
        })?;
        let reader = Arc::new(admit_generation(candidate).await?);
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
            block_cache,
            instance_memory_throttle: ResourceGaugeThrottle::for_instance_memory(),
            writer_attestation,
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
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(refresh_interval) => {}
                    () = self.writer_attestation.stale_deadline().passed() => {}
                }
                let deadline = self.writer_attestation.stale_deadline();
                let admission = self.readiness.composed.clone();
                refresh_within_stale_deadline(deadline, &admission, self.refresh()).await;
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
    async fn refresh(&mut self) {
        let active = self.slot.capture();
        let status = match self.writer_status.fetch().await {
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
            CanonicalFenceRelation::Matches => {
                self.writer_attestation.attest();
                self.readiness.publish_storage(ReadinessState::ready(Some(
                    active.event_fence().visible_tip().height.value(),
                )));
                return;
            }
            CanonicalFenceRelation::SecondaryBehind(behind) => behind,
        };
        let lag = behind.lag_chain_epochs;
        if lag <= self.config.lag_threshold {
            self.readiness
                .publish_storage(ReadinessState::ready_with_target(
                    Some(active.event_fence().visible_tip().height.value()),
                    Some(behind.writer_visible_tip_height),
                ));
        } else if let Some(staleness) = self.writer_attestation.stale_allowance() {
            self.readiness
                .publish_storage(ReadinessState::serving_pair_stale(
                    lag,
                    staleness.as_secs(),
                    Some(active.event_fence().visible_tip().height.value()),
                ));
        } else {
            self.record_failure(
                ReadinessCause::ReplicaLagging {
                    lag_chain_epochs: lag,
                },
                active.event_fence().visible_tip().height.value(),
            );
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
                block_cache: &self.block_cache,
                generation: candidate_generation,
            },
            ServedReader {
                attestation: &mut self.writer_attestation,
                readiness: &self.readiness,
                visible_height: Some(active.event_fence().visible_tip().height.value()),
            },
        )
        .await;
        // A halt or store damage a status read during convergence reported
        // outlasts the failure that ended it.
        if let (Err(_), Some(halt)) = (&converged, self.writer_attestation.remembered_halt()) {
            self.readiness.publish_storage(halt);
            return;
        }
        let (candidate, attesting_status) = match converged {
            Ok(converged) => converged,
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
            Err(ConvergenceFailure::Storage(_) | ConvergenceFailure::WriterScheduleNotAdmitted) => {
                self.record_failure(
                    ReadinessCause::StorageUnavailable,
                    active.event_fence().visible_tip().height.value(),
                );
                return;
            }
        };
        let Ok(reader) = admit_generation(candidate).await else {
            self.record_failure(
                ReadinessCause::StorageUnavailable,
                active.event_fence().visible_tip().height.value(),
            );
            return;
        };
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
        }
        self.readiness.publish_storage(
            refusal.unwrap_or_else(|| ReadinessState::ready(Some(visible_height))),
        );
    }

    /// Publishes the shared block cache and, once per throttle interval, the
    /// summed memory of the active generation and of a replaced one that
    /// requests still hold.
    ///
    /// The memory sample looks up every table file's reader, so it runs on
    /// the blocking pool instead of the publisher task.
    async fn record_resource_gauges(&self) {
        record_shared_block_cache_gauges(StoreRole::CanonicalSecondary, &self.block_cache);
        if !self.instance_memory_throttle.should_sample() {
            return;
        }
        let generations = std::iter::once(self.slot.capture())
            .chain(self.retired.clone())
            .collect::<Vec<_>>();
        let sampled = tokio::task::spawn_blocking(move || {
            generations
                .iter()
                .map(|generation| generation.secondary().instance_memory())
                .sum()
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
        block_cache,
        generation,
    } = candidate;
    let primary = config.canonical_primary_path.clone();
    let secondary_root = config.canonical_secondary_root.clone();
    let network = config.network;
    let retention = config.raw_blob_retention;
    let reorg = config.reorg_policy;
    let resources =
        RocksDbOpenResources::with_shared_block_cache(config.resource_budget, block_cache);
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
            CanonicalStoreWorkload::Wallet,
            retention,
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
) -> Result<RocksDbCanonicalReader, CompactBlockServingError> {
    tokio::task::spawn_blocking(move || RocksDbCanonicalReader::new(candidate))
        .await
        .map_err(|error| {
            CompactBlockServingError::Runtime(format!(
                "compact-block admission task failed: {error}"
            ))
        })?
        .map_err(CompactBlockServingError::Admission)
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
    block_cache: &'a SharedBlockCache,
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
    /// Visible tip height of the active reader, or `None` at bootstrap.
    visible_height: Option<u32>,
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

    /// A halt or store damage the writer reports while the candidate trails
    /// refuses traffic on the active reader at once.
    fn observe_writer_status(&mut self, status: &CanonicalWriterStatusResponse) {
        if let Some(refusal) = self
            .served
            .attestation
            .observe(status, self.served.visible_height)
        {
            self.served.readiness.publish_storage(refusal);
        }
    }
}

fn candidate_unavailable() -> ConvergenceFailure {
    ConvergenceFailure::Storage(CompactBlockServingError::Runtime(
        "compact-block candidate is unavailable".to_owned(),
    ))
}

/// Opens a candidate in `generation`, retrying until `convergence_timeout`
/// passes while the canonical writer has not admitted its first
/// network-upgrade schedule.
///
/// The writer admits that schedule when it first opens the store, which can
/// take longer than the convergence attempts allow, so this wait is bounded
/// by time alone.
async fn open_generation_awaiting_schedule(
    config: &CompactBlockServingConfig,
    generation: CandidateGeneration<'_>,
    retry_delay: Duration,
) -> Result<RocksDbCanonicalSecondary, ConvergenceFailure> {
    let deadline = tokio::time::Instant::now() + config.convergence_timeout;
    let mut waiting_logged = false;
    loop {
        match open_generation(config, generation).await {
            Ok(candidate) => return Ok(candidate),
            Err(CompactBlockServingError::Storage(
                CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted { .. },
            )) if tokio::time::Instant::now() < deadline => {
                if !waiting_logged {
                    tracing::info!(
                        target: "zinder::compat_lightwalletd",
                        event = "canonical_schedule_admission_pending",
                        "waiting for the canonical writer to admit a network-upgrade schedule"
                    );
                    waiting_logged = true;
                }
            }
            Err(CompactBlockServingError::Storage(
                CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted { .. },
            )) => return Err(ConvergenceFailure::WriterScheduleNotAdmitted),
            Err(error) => return Err(ConvergenceFailure::Storage(error)),
        }
        tokio::time::sleep(retry_delay).await;
    }
}

/// Opens a candidate in `generation`, converges it on the writer fence, and
/// returns it with the writer status that attested it.
async fn converge_candidate<'a>(
    writer_status: &'a mut WriterStatus,
    config: &CompactBlockServingConfig,
    generation: CandidateGeneration<'_>,
    served: ServedReader<'a>,
) -> Result<(RocksDbCanonicalSecondary, CanonicalWriterStatusResponse), ConvergenceFailure> {
    let retry_delay = config.catchup_interval.min(CONVERGENCE_RETRY_DELAY_CAP);
    let candidate = open_generation_awaiting_schedule(config, generation, retry_delay).await?;
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
        &CancellationToken::new(),
    )
    .await;
    match outcome {
        Ok(CanonicalFenceConvergence::Converged(attesting_status)) => probe
            .candidate
            .map(|candidate| (candidate, *attesting_status))
            .ok_or_else(candidate_unavailable),
        Ok(CanonicalFenceConvergence::Cancelled | CanonicalFenceConvergence::Trailing(_))
        | Err(CanonicalFenceConvergenceError::DidNotConverge) => {
            Err(ConvergenceFailure::WriterAhead)
        }
        Err(CanonicalFenceConvergenceError::Probe(failure)) => Err(failure),
        Err(CanonicalFenceConvergenceError::Diverged(_)) => {
            Err(ConvergenceFailure::WriterFenceDiverged)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{
        CompactBlockPublisher, CompactBlockServingConfig, CompactBlockServingError,
        CompactServingReadiness, transport_failure_cause,
    };
    use compact_tx_streamer_server::CompactTxStreamer;
    use parking_lot::Mutex;
    use prost::Message;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Request, transport::Server};
    use zinder_compat_lightwalletd::{CanonicalReaderSlot, CompactBlocksGrpcAdapter};
    use zinder_core::{
        BlockHeight, BlockId, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockReplayFormatVersion, ChainTipMetadata, CommitmentTreeCheckpoint,
        CommitmentTreeFrontier, CommitmentTreeFrontiers, ConsensusBranchId, Network,
        NetworkUpgradeActivation, NetworkUpgradeActivations, SerializedBytesDigest,
        ShieldedProtocol, UnixTimestampMillis, encode_canonical_block_replay,
    };
    use zinder_proto::compat::lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient;
    use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server};
    use zinder_proto::v1::ingest::{
        AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
        CanonicalEventPageResponse, CanonicalProjectionBuildLeaseResponse, CanonicalWriterFence,
        CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
        CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
        ReadmitCanonicalOwnerCheckpointRequest, ReleaseCanonicalProjectionBuildLeaseRequest,
        ReleaseCanonicalProjectionBuildLeaseResponse, RenewCanonicalProjectionBuildLeaseRequest,
        canonical_control_server::{CanonicalControl, CanonicalControlServer},
    };
    use zinder_query::{CanonicalReader, RocksDbCanonicalReader};
    use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalLiveAppend,
        CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention,
        ResourceGaugeThrottle, RocksDbCanonicalBuilder, RocksDbCanonicalStore,
        RocksDbResourceBudget,
    };

    fn assert_unimplemented<T>(result: Result<T, tonic::Status>) {
        assert!(result.is_err());
        let Some(status) = result.err() else {
            return;
        };
        assert_eq!(status.code(), Code::Unimplemented);
    }

    #[derive(Clone)]
    struct MutableCanonicalControl {
        writer_status: Arc<Mutex<CanonicalWriterStatusResponse>>,
        /// Answers served once each, in order, before `writer_status`; `None`
        /// answers with an outage.
        queued_statuses:
            Arc<Mutex<std::collections::VecDeque<Option<CanonicalWriterStatusResponse>>>>,
        available: Arc<std::sync::atomic::AtomicBool>,
        /// Accepts each writer-status call and never answers it.
        stalled: Arc<std::sync::atomic::AtomicBool>,
        /// How long each writer-status call waits before it answers.
        answer_delay: Arc<Mutex<Duration>>,
    }

    impl MutableCanonicalControl {
        fn new(writer_status: CanonicalWriterStatusResponse) -> Self {
            Self {
                writer_status: Arc::new(Mutex::new(writer_status)),
                queued_statuses: Arc::new(Mutex::new(std::collections::VecDeque::new())),
                available: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                stalled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                answer_delay: Arc::new(Mutex::new(Duration::ZERO)),
            }
        }

        fn stall(&self) {
            self.stalled
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn delay_answers(&self, delay: Duration) {
            *self.answer_delay.lock() = delay;
        }

        fn queue_writer_status(&self, writer_status: CanonicalWriterStatusResponse) {
            self.queued_statuses.lock().push_back(Some(writer_status));
        }

        fn queue_outage(&self) {
            self.queued_statuses.lock().push_back(None);
        }

        fn set_writer_status(&self, writer_status: CanonicalWriterStatusResponse) {
            *self.writer_status.lock() = writer_status;
        }

        fn set_available(&self, available: bool) {
            self.available
                .store(available, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[tonic::async_trait]
    impl CanonicalControl for MutableCanonicalControl {
        async fn writer_status(
            &self,
            _request: Request<CanonicalWriterStatusRequest>,
        ) -> Result<tonic::Response<CanonicalWriterStatusResponse>, tonic::Status> {
            if self.stalled.load(std::sync::atomic::Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            let answer_delay = *self.answer_delay.lock();
            if !answer_delay.is_zero() {
                tokio::time::sleep(answer_delay).await;
            }
            if !self.available.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(tonic::Status::unavailable("fixture writer status outage"));
            }
            let queued = self.queued_statuses.lock().pop_front();
            match queued {
                Some(Some(writer_status)) => Ok(tonic::Response::new(writer_status)),
                Some(None) => Err(tonic::Status::unavailable(
                    "fixture queued writer status outage",
                )),
                None => Ok(tonic::Response::new(self.writer_status.lock().clone())),
            }
        }

        async fn event_page(
            &self,
            _request: Request<CanonicalEventPageRequest>,
        ) -> Result<tonic::Response<CanonicalEventPageResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn create_owner_checkpoint(
            &self,
            _request: Request<CreateCanonicalOwnerCheckpointRequest>,
        ) -> Result<tonic::Response<CreateCanonicalOwnerCheckpointResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn readmit_owner_checkpoint(
            &self,
            _request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
        ) -> Result<tonic::Response<CreateCanonicalOwnerCheckpointResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn acquire_projection_build_lease(
            &self,
            _request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<tonic::Response<CanonicalProjectionBuildLeaseResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn renew_projection_build_lease(
            &self,
            _request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<tonic::Response<CanonicalProjectionBuildLeaseResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }

        async fn release_projection_build_lease(
            &self,
            _request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
        ) -> Result<tonic::Response<ReleaseCanonicalProjectionBuildLeaseResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "fixture only serves writer status",
            ))
        }
    }

    fn lifecycle_build_block(
        block: &zinder_testkit::FixtureBlock,
        activations: &NetworkUpgradeActivations,
    ) -> Result<CanonicalBuildBlock, zinder_core::CommitmentTreeFrontiersError> {
        let facts = CanonicalBlockFacts {
            block_header: block.block_header_artifact(),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &block.raw_block_bytes,
            ),
            transactions: Vec::new(),
        };
        Ok(CanonicalBuildBlock {
            replay_envelope: encode_canonical_block_replay(
                &facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            ),
            compact_block: block.compact_block_artifact(),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint: Some(CommitmentTreeCheckpoint::new(
                BlockId::new(block.height, block.hash),
                block.block_time_seconds,
                lifecycle_frontiers(activations, block.height)?,
            )),
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
            facts,
        })
    }

    /// Live following derives each checkpoint's frontiers from its
    /// predecessor, so every shielded pool active at `height` carries an
    /// empty frontier.
    fn lifecycle_frontiers(
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> Result<CommitmentTreeFrontiers, zinder_core::CommitmentTreeFrontiersError> {
        let active_frontier = |protocol: ShieldedProtocol| {
            activations
                .activation_height_by_name(protocol.activation_upgrade_name())
                .is_some_and(|activation_height| activation_height <= height)
                .then(|| CommitmentTreeFrontier::empty(protocol))
        };
        CommitmentTreeFrontiers::from_validated_parts(
            active_frontier(ShieldedProtocol::Sapling),
            active_frontier(ShieldedProtocol::Orchard),
            active_frontier(ShieldedProtocol::Ironwood),
        )
    }

    fn lifecycle_activations() -> NetworkUpgradeActivations {
        let upgrades = [
            ("Overwinter", 1_u32),
            ("Sapling", 2),
            ("Blossom", 3),
            ("Heartwood", 4),
            ("Canopy", 5),
            ("NU5", 6),
            ("NU6", 7),
            ("NU6.1", 8),
            ("NU6.2", 9),
            ("NU6.3", 10),
        ]
        .into_iter()
        .map(|(name, branch_id)| NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(branch_id),
            activation_height: BlockHeight::new(100),
            name: name.to_owned(),
        })
        .collect();
        NetworkUpgradeActivations::new(Network::ZcashRegtest, upgrades)
            .unwrap_or_else(|_| NetworkUpgradeActivations::empty(Network::ZcashRegtest))
    }

    fn lifecycle_writer_status(store: &RocksDbCanonicalStore) -> CanonicalWriterStatusResponse {
        let fence = store.event_fence();
        CanonicalWriterStatusResponse {
            network_name: zinder_core::wire::encode_zinder_native_chain_name(Network::ZcashRegtest)
                .to_owned(),
            fence: Some(CanonicalWriterFence {
                chain_epoch_id: fence.chain_epoch_id().value(),
                event_sequence: fence.chain_event_sequence(),
                visible_tip_height: fence.visible_tip().height.value(),
                visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
                visible_block_count: fence.sequence_digest().block_count(),
                canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
            }),
            oldest_retained_event_sequence: 1,
            network_upgrade_schedule: store.network_upgrade_schedule().map(|schedule| {
                zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                    schedule.activations(),
                    schedule.revision(),
                )
            }),
            reorg_window_exceeded: None,
            tip_unconfirmed: false,
            canonical_store_damaged: None,
        }
    }

    /// Builds the lifecycle store and admits `activations` as schedule
    /// revision 1, as the canonical writer does when it first opens it.
    fn lifecycle_store(
        root: &std::path::Path,
        activations: &zinder_core::NetworkUpgradeActivations,
    ) -> Result<
        (RocksDbCanonicalStore, Vec<zinder_testkit::FixtureBlock>),
        Box<dyn std::error::Error>,
    > {
        let (mut store, blocks) = unadmitted_lifecycle_store(root, activations)?;
        store.admit_network_upgrade_schedule(activations)?;
        Ok((store, blocks))
    }

    /// Builds the READY lifecycle store before its writer's first schedule
    /// admission.
    fn unadmitted_lifecycle_store(
        root: &std::path::Path,
        activations: &zinder_core::NetworkUpgradeActivations,
    ) -> Result<
        (RocksDbCanonicalStore, Vec<zinder_testkit::FixtureBlock>),
        Box<dyn std::error::Error>,
    > {
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut blocks = chain.blocks().to_vec();
        let first = blocks
            .first_mut()
            .ok_or("lifecycle fixture has no baseline block")?;
        first.parent_hash = Network::ZcashRegtest.genesis_hash();
        let first = first.clone();
        let builder_plan = CanonicalStoreBuildPlan::complete(
            activations,
            first.block_time_seconds.saturating_sub(1),
            BlockId::new(first.height, first.hash),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            root,
            CanonicalStoreWorkload::Wallet,
            builder_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks(
            activations,
            std::iter::once(Ok::<_, std::io::Error>(lifecycle_build_block(
                &first,
                activations,
            )?)),
        )?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            BlockId::new(first.height, first.hash),
            first.block_time_seconds,
            lifecycle_frontiers(activations, first.height)?,
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            BlockId::new(first.height, first.hash),
            UnixTimestampMillis::new(u64::from(first.block_time_seconds) * 1_000),
        ))?;
        Ok((validated.publish_baseline(publication)?, blocks))
    }

    #[test]
    fn readiness_follows_the_published_generation_and_shutdown_is_irreversible() {
        let runtime = Readiness::default();
        let readiness = CompactServingReadiness::new(runtime.clone());
        assert!(!runtime.report().is_ready);
        readiness.publish_storage(ReadinessState::ready(Some(10)));
        let report = runtime.report();
        assert!(report.is_ready);
        assert_eq!(
            report.degraded,
            [] as [zinder_runtime::ReadinessDependency; 0]
        );
        readiness.publish_shutting_down();
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
        readiness.publish_storage(ReadinessState::ready(Some(11)));
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
    }

    #[tokio::test]
    async fn bootstrap_rejects_a_writer_fence_with_a_different_visible_block_count()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let mut writer_status = lifecycle_writer_status(&primary);
        let fence = writer_status
            .fence
            .as_mut()
            .ok_or("lifecycle writer status has no fence")?;
        fence.visible_block_count = fence.visible_block_count.saturating_add(1);
        let control = MutableCanonicalControl::new(writer_status);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(CanonicalControlServer::new(control))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await
        });
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_millis(50),
            lag_threshold: 0,
        };

        let outcome = CompactBlockPublisher::bootstrap(
            config,
            CompactServingReadiness::new(Readiness::default()),
            &endpoint,
            None,
        )
        .await;

        let bootstrap_error = outcome.err();
        let _ = shutdown_sender.send(());
        server.await??;
        let Some(CompactBlockServingError::Runtime(message)) = bootstrap_error else {
            return Err(
                format!("expected a runtime bootstrap error, got {bootstrap_error:?}").into(),
            );
        };
        assert!(
            message.contains("writer fence diverged from the candidate secondary"),
            "bootstrap failed for a reason other than fence divergence: {message}"
        );
        Ok(())
    }

    /// Compact serving brings no activation table of its own: bootstrap waits
    /// for the writer's first schedule admission and serves that schedule.
    ///
    /// The wait is bounded by the convergence timeout, however many attempts
    /// it takes.
    #[tokio::test]
    async fn bootstrap_waits_for_the_writers_first_schedule_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (mut primary, _blocks) =
            unadmitted_lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(10),
            convergence_attempts: 1,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let bootstrap = tokio::spawn(async move {
            CompactBlockPublisher::bootstrap(
                config,
                CompactServingReadiness::new(Readiness::default()),
                &endpoint,
                None,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !bootstrap.is_finished(),
            "bootstrap must wait for the schedule"
        );
        primary.admit_network_upgrade_schedule(&activations)?;
        control.set_writer_status(lifecycle_writer_status(&primary));
        let (_publisher, slot) = bootstrap.await??;

        let reader = slot.capture();
        assert_eq!(
            reader.secondary().network_upgrade_activations(),
            Some(&activations)
        );
        let lightd_info_response = CompactBlocksGrpcAdapter::new(slot.clone())
            .get_lightd_info(Request::new(lightwalletd::Empty {}))
            .await?
            .into_inner();
        assert_eq!(
            lightd_info_response.chain_name,
            zinder_core::wire::encode_bip70_chain_name(Network::ZcashRegtest)
        );
        drop(reader);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// A writer that never admits a schedule fails bootstrap with a typed
    /// error instead of a reader-supplied table.
    #[tokio::test]
    async fn bootstrap_reports_a_store_whose_writer_admitted_no_schedule()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            unadmitted_lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(CanonicalControlServer::new(control))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await
        });
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_millis(300),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };

        let outcome = CompactBlockPublisher::bootstrap(
            config,
            CompactServingReadiness::new(Readiness::default()),
            &endpoint,
            None,
        )
        .await;

        let bootstrap_error = outcome.err();
        let _ = shutdown_sender.send(());
        server.await??;
        let Some(CompactBlockServingError::Runtime(message)) = bootstrap_error else {
            return Err(
                format!("expected a runtime bootstrap error, got {bootstrap_error:?}").into(),
            );
        };
        assert!(
            message.contains("has not admitted a network-upgrade schedule"),
            "bootstrap failed for another reason: {message}"
        );
        Ok(())
    }

    /// A writer that never answers its status cannot hold a refresh open.
    ///
    /// Each read ends at the wallet ingest-control bound, and once the
    /// attestation passes the staleness ceiling the publisher refuses traffic
    /// with `writer_status_unavailable`.
    #[tokio::test]
    async fn a_writer_that_never_answers_its_status_reaches_the_staleness_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
            }
        });
        let readiness = Readiness::default();
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_millis(50),
            lag_threshold: 0,
        };
        let (mut publisher, _slot) = CompactBlockPublisher::bootstrap(
            config,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        control.stall();
        tokio::time::pause();
        let refreshed = tokio::time::timeout(
            zinder_query::WALLET_INGEST_CONTROL_REQUEST_TIMEOUT * 2,
            publisher.refresh(),
        )
        .await;
        tokio::time::resume();

        assert!(
            refreshed.is_ok(),
            "a refresh waited on a writer status the writer never answered"
        );
        assert_eq!(
            readiness.report().cause,
            ReadinessCause::WriterStatusUnavailable
        );
        drop(publisher);
        drop(primary);
        server.abort();
        let _join_outcome = server.await;
        Ok(())
    }

    /// Both compact-serving generations open on one block cache.
    ///
    /// The generation a refresh opens cold finds the blocks the active one
    /// loaded, so reading them adds nothing to the cache. While a request
    /// holds the replaced generation, the cache reports the configured
    /// capacity once and the memtable gauge sums both open generations. The
    /// memtable gauge changes only when the memory-sample throttle admits.
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The test keeps the writer setup, the cold refresh, the cache reads, and the exported gauges in one causal path, as the lifecycle test does."
    )]
    async fn generations_open_on_one_block_cache() -> Result<(), Box<dyn std::error::Error>> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let rendered_metrics = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let serving_readiness = CompactServingReadiness::new(Readiness::default());
        let budget = RocksDbResourceBudget::for_local_tests();
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: budget,
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let block_cache = publisher.block_cache.clone();
        let initial = slot.capture();
        let first_block = initial.compact_block_at(BlockHeight::new(1)).await?;
        let loaded = block_cache.usage_bytes();
        assert!(
            loaded > 0,
            "the active generation must read through the publisher's cache"
        );

        let second = blocks
            .get(1)
            .cloned()
            .ok_or("lifecycle fixture has no append block")?;
        let expected_fence = primary.event_fence();
        let (primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&second, &activations)?,
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(2_000),
            ),
            &activations,
        )?;
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        let replaced = slot.capture();
        assert!(!Arc::ptr_eq(&initial, &replaced));
        let before_read = block_cache.usage_bytes();
        assert!(
            before_read >= loaded,
            "a cold open on refresh keeps the blocks already loaded"
        );
        assert_eq!(
            replaced.compact_block_at(BlockHeight::new(1)).await?,
            first_block
        );
        assert_eq!(
            block_cache.usage_bytes(),
            before_read,
            "the new generation reads the block the replaced one loaded"
        );

        let gauge = |name: &str| {
            rendered_metrics.render().lines().find_map(|line| {
                line.strip_prefix(name)?
                    .strip_prefix("{store_role=\"canonical_secondary\"} ")?
                    .parse::<f64>()
                    .ok()
            })
        };
        let generation_memtable_budget = u32::try_from(budget.memtable_budget_bytes)?;
        publisher.record_resource_gauges().await;
        assert_eq!(
            gauge("zinder_store_memtable_budget_bytes"),
            Some(f64::from(generation_memtable_budget)),
            "the memory sample waits for its throttle interval after bootstrap"
        );
        publisher.instance_memory_throttle = ResourceGaugeThrottle::new(Duration::ZERO);
        publisher.record_resource_gauges().await;
        assert_eq!(
            gauge("zinder_store_block_cache_capacity_bytes"),
            Some(f64::from(u32::try_from(budget.block_cache_bytes)?))
        );
        assert_eq!(
            gauge("zinder_store_block_cache_usage_bytes"),
            Some(f64::from(u32::try_from(block_cache.usage_bytes())?))
        );
        assert_eq!(
            gauge("zinder_store_memtable_budget_bytes"),
            Some(f64::from(2 * generation_memtable_budget)),
            "the active and the held generation each keep their memtables"
        );

        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_handle = publisher.spawn(cancel.clone(), drained.clone());
        cancel.cancel();
        drained.cancel();
        publisher_handle.await??;
        drop((initial, replaced, slot));
        assert_eq!(block_cache.pinned_usage_bytes(), 0);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The lifecycle test keeps admission, publication, lease retention, outage, and identity rejection in one deterministic causal path."
    )]
    async fn publisher_lifecycle_replaces_and_preserves_readiness_while_leased()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (mut primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });

        let staleness_ceiling = Duration::from_mins(1);
        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling,
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let initial = slot.capture();
        assert_eq!(
            initial.event_fence().visible_tip().height,
            BlockHeight::new(1)
        );

        let second = blocks
            .get(1)
            .cloned()
            .ok_or("lifecycle fixture has no append block")?;
        let expected_fence = primary.event_fence();
        let (next_primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&second, &activations)?,
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(2_000),
            ),
            &activations,
        )?;
        primary = next_primary;
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        let replaced = slot.capture();
        assert_eq!(
            replaced.event_fence().visible_tip().height,
            BlockHeight::new(2)
        );
        assert!(!Arc::ptr_eq(&initial, &replaced));

        let third = blocks
            .get(2)
            .cloned()
            .ok_or("lifecycle fixture has no second append block")?;
        let expected_fence = primary.event_fence();
        let (next_primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&third, &activations)?,
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(3_000),
            ),
            &activations,
        )?;
        primary = next_primary;
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause.metric_label(),
            "serving_pair_stale"
        );
        assert_eq!(
            slot.capture().event_fence().visible_tip().height,
            BlockHeight::new(2)
        );
        assert_eq!(
            initial.event_fence().visible_tip().height,
            BlockHeight::new(1)
        );

        control.set_available(false);
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause.metric_label(),
            "serving_pair_stale"
        );
        // Advancing the paused clock past the ceiling expires the allowance
        // while the lease and the confirmed writer status stay as they are, so
        // no wall-clock wait or machine load decides the outcome.
        control.set_available(true);
        control.set_writer_status(lifecycle_writer_status(&primary));
        tokio::time::pause();
        tokio::time::advance(staleness_ceiling + Duration::from_secs(1)).await;
        tokio::time::resume();
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "replica_lagging");

        control.set_available(false);
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause.metric_label(),
            "writer_status_unavailable"
        );

        control.set_available(true);
        let mut invalid = lifecycle_writer_status(&primary);
        invalid.network_name = "wrong-network".to_owned();
        control.set_writer_status(invalid);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        let mut missing_fence = lifecycle_writer_status(&primary);
        missing_fence.fence = None;
        control.set_writer_status(missing_fence);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        let mut behind = lifecycle_writer_status(&primary);
        if let Some(fence) = behind.fence.as_mut() {
            fence.chain_epoch_id = 0;
        }
        control.set_writer_status(behind);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        let mut same_epoch_mismatch = lifecycle_writer_status(&primary);
        if let Some(fence) = same_epoch_mismatch.fence.as_mut() {
            fence.chain_epoch_id = 2;
            fence.visible_tip_height = fence.visible_tip_height.saturating_add(1);
        }
        control.set_writer_status(same_epoch_mismatch);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause.metric_label(), "schema_mismatch");

        drop(initial);
        control.set_available(true);
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        let reused = slot.capture();
        assert_eq!(
            reused.event_fence().visible_tip().height,
            BlockHeight::new(3)
        );
        assert!(!Arc::ptr_eq(&replaced, &reused));

        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_handle = publisher.spawn(cancel.clone(), drained.clone());
        cancel.cancel();
        drained.cancel();
        publisher_handle.await??;
        drop(replaced);
        drop(reused);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// Compact serving refuses at once while the writer reports store damage.
    ///
    /// It refuses on the next refresh, keeps refusing through a
    /// writer-status outage, and admits traffic again once a status reports
    /// no damage.
    #[tokio::test]
    async fn publisher_refuses_at_once_while_the_writer_reports_store_damage()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let (endpoint, shutdown_sender, server) = serve_control(&control).await?;
        let readiness = Readiness::default();
        let (mut publisher, slot) = CompactBlockPublisher::bootstrap(
            compact_serving_config(&temporary, Duration::from_millis(1), Duration::from_mins(1))?,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        control.set_writer_status(CanonicalWriterStatusResponse {
            canonical_store_damaged: Some(zinder_proto::wire::canonical_store_damaged_detail(
                Some(BlockHeight::new(1)),
                None,
            )),
            ..lifecycle_writer_status(&primary)
        });
        publisher.refresh().await;
        let damaged = ReadinessCause::CanonicalStoreDamaged {
            height: Some(1),
            pool: None,
        };
        assert_eq!(readiness.report().cause, damaged);
        assert!(!readiness.report().is_ready);

        control.set_available(false);
        publisher.refresh().await;
        assert_eq!(readiness.report().cause, damaged);

        control.set_available(true);
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert!(readiness.report().is_ready);

        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// Store damage reported while a candidate trails the writer outlasts an
    /// outage.
    ///
    /// The damage refuses traffic at once, and a writer-status outage that
    /// then ends the refresh keeps refusing instead of admitting traffic
    /// under the staleness allowance.
    #[tokio::test]
    async fn damage_reported_while_a_candidate_trails_survives_a_later_outage()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let (endpoint, shutdown_sender, server) = serve_control(&control).await?;
        let readiness = Readiness::default();
        let (mut publisher, slot) = CompactBlockPublisher::bootstrap(
            compact_serving_config(&temporary, Duration::from_millis(1), Duration::from_mins(1))?,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        // The first status shows the writer one epoch ahead, so the refresh
        // converges a candidate that trails it; the writer reports damage on
        // the next status and then stops answering.
        let mut ahead = lifecycle_writer_status(&primary);
        let fence = ahead
            .fence
            .as_mut()
            .ok_or("lifecycle writer status has no fence")?;
        fence.chain_epoch_id += 1;
        fence.event_sequence += 1;
        fence.visible_tip_height += 1;
        control.queue_writer_status(ahead.clone());
        control.queue_writer_status(CanonicalWriterStatusResponse {
            canonical_store_damaged: Some(zinder_proto::wire::canonical_store_damaged_detail(
                Some(BlockHeight::new(1)),
                None,
            )),
            ..ahead
        });
        control.queue_outage();
        publisher.refresh().await;

        assert_eq!(
            readiness.report().cause,
            ReadinessCause::CanonicalStoreDamaged {
                height: Some(1),
                pool: None,
            }
        );
        assert!(!readiness.report().is_ready);
        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// The staleness ceiling holds when the catch-up interval is longer.
    ///
    /// The publisher stops admitting traffic on an unattested reader once
    /// the ceiling passes, before its next scheduled refresh.
    #[tokio::test]
    async fn the_staleness_ceiling_holds_with_a_longer_catch_up_interval()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_millis(200);
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let (endpoint, shutdown_sender, server) = serve_control(&control).await?;
        let readiness = Readiness::default();
        let (publisher, slot) = CompactBlockPublisher::bootstrap(
            compact_serving_config(&temporary, Duration::from_hours(1), staleness_ceiling)?,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        control.set_available(false);
        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), drained.clone());
        tokio::time::sleep(staleness_ceiling * 4).await;
        let after_ceiling = readiness.report();
        cancel.cancel();
        drained.cancel();
        publisher_task.await??;

        assert!(
            !after_ceiling.is_ready,
            "an unattested reader must stop admitting traffic at the ceiling: {:?}",
            after_ceiling.cause
        );
        drop(slot);
        drop(primary);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// A writer-status read that stalls cannot hold admission past the
    /// staleness ceiling.
    ///
    /// Each read waits until the wallet ingest-control bound ends it, so a
    /// read is always in flight when the ceiling passes; the reader stops
    /// admitting traffic at the ceiling instead of when that read ends.
    #[tokio::test]
    async fn a_stalled_writer_status_read_ends_admission_at_the_staleness_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_mins(1);
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let (endpoint, shutdown_sender, server) = serve_control(&control).await?;
        let readiness = Readiness::default();
        let (publisher, slot) = CompactBlockPublisher::bootstrap(
            compact_serving_config(&temporary, Duration::from_secs(1), staleness_ceiling)?,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        control.stall();
        tokio::time::pause();
        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), drained.clone());
        tokio::time::sleep(Duration::from_secs(59)).await;
        let before_ceiling = readiness.report();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after_ceiling = readiness.report();
        cancel.cancel();
        drained.cancel();
        tokio::time::timeout(
            zinder_query::WALLET_INGEST_CONTROL_REQUEST_TIMEOUT * 4,
            publisher_task,
        )
        .await???;
        tokio::time::resume();

        assert!(before_ceiling.is_ready, "{:?}", before_ceiling.cause);
        assert_eq!(after_ceiling.cause, ReadinessCause::WriterStatusUnavailable);
        drop(slot);
        drop(primary);
        drop(shutdown_sender);
        server.abort();
        let _join_outcome = server.await;
        Ok(())
    }

    /// A publisher that starts its loop after the ceiling has passed ends
    /// admission at once instead of after its catch-up interval.
    #[tokio::test]
    async fn a_publisher_that_starts_after_the_staleness_ceiling_ends_admission_at_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_mins(1);
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let (endpoint, shutdown_sender, server) = serve_control(&control).await?;
        let readiness = Readiness::default();
        let (publisher, slot) = CompactBlockPublisher::bootstrap(
            compact_serving_config(&temporary, Duration::from_hours(1), staleness_ceiling)?,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;

        control.stall();
        tokio::time::pause();
        tokio::time::advance(staleness_ceiling + Duration::from_secs(1)).await;
        assert!(readiness.report().is_ready);
        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), drained.clone());
        tokio::time::sleep(Duration::from_millis(1)).await;
        let at_start = readiness.report();
        cancel.cancel();
        drained.cancel();
        tokio::time::timeout(
            zinder_query::WALLET_INGEST_CONTROL_REQUEST_TIMEOUT * 4,
            publisher_task,
        )
        .await???;
        tokio::time::resume();

        assert_eq!(at_start.cause, ReadinessCause::WriterStatusUnavailable);
        drop(slot);
        drop(primary);
        drop(shutdown_sender);
        server.abort();
        let _join_outcome = server.await;
        Ok(())
    }

    /// A writer that answers keeps a reader admitting traffic when the
    /// catch-up interval is longer than the staleness ceiling.
    ///
    /// Each answer takes 100 ms, so a refresh that started only once the
    /// ceiling passed would refuse traffic until the writer answered it. The
    /// test runs on the real clock because the paused clock can advance past
    /// a pending socket read.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_answering_writer_keeps_admission_past_the_staleness_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let staleness_ceiling = Duration::from_secs(1);
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let (endpoint, shutdown_sender, server) = serve_control(&control).await?;
        let readiness = Readiness::default();
        let (publisher, slot) = CompactBlockPublisher::bootstrap(
            compact_serving_config(&temporary, Duration::from_hours(1), staleness_ceiling)?,
            CompactServingReadiness::new(readiness.clone()),
            &endpoint,
            None,
        )
        .await?;
        assert!(readiness.report().is_ready);

        control.delay_answers(Duration::from_millis(100));
        let cancel = CancellationToken::new();
        let drained = CancellationToken::new();
        let publisher_task = publisher.spawn(cancel.clone(), drained.clone());
        let mut refusals = Vec::new();
        let sampled_until = tokio::time::Instant::now() + staleness_ceiling * 3;
        while tokio::time::Instant::now() < sampled_until {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let report = readiness.report();
            if !report.is_ready {
                refusals.push(report.cause);
            }
        }
        cancel.cancel();
        drained.cancel();
        publisher_task.await??;

        assert!(
            refusals.is_empty(),
            "an answering writer must keep the reader admitted: {refusals:?}"
        );
        drop(slot);
        drop(primary);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// Serves `control` until the returned sender fires.
    async fn serve_control(
        control: &MutableCanonicalControl,
    ) -> Result<
        (
            String,
            oneshot::Sender<()>,
            tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        Ok((endpoint, shutdown_sender, server))
    }

    fn compact_serving_config(
        temporary: &tempfile::TempDir,
        catchup_interval: Duration,
        staleness_ceiling: Duration,
    ) -> Result<CompactBlockServingConfig, Box<dyn std::error::Error>> {
        Ok(CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval,
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling,
            lag_threshold: 0,
        })
    }

    /// Compact serving refuses a writer that has not confirmed its tip since
    /// it reopened.
    ///
    /// It stays unready with `canonical_tip_unconfirmed`, never
    /// `serving_pair_stale`, through a writer-status outage, keeps a
    /// remembered halt while the flag is set, and turns ready only once a
    /// status carries neither.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "bootstrap, outage, recovery, a park, and the restarted writer form one causal sequence"
    )]
    async fn publisher_refuses_a_writer_with_an_unconfirmed_tip()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let unconfirmed_status = CanonicalWriterStatusResponse {
            tip_unconfirmed: true,
            ..lifecycle_writer_status(&primary)
        };
        let control = MutableCanonicalControl::new(unconfirmed_status.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        assert_eq!(
            readiness.report().cause,
            ReadinessCause::CanonicalTipUnconfirmed
        );
        assert!(!readiness.report().is_ready);

        control.set_available(false);
        publisher.refresh().await;
        let during_outage = readiness.report();
        assert!(!during_outage.is_ready);
        assert_ne!(during_outage.cause.metric_label(), "serving_pair_stale");

        control.set_available(true);
        publisher.refresh().await;
        assert_eq!(
            readiness.report().cause,
            ReadinessCause::CanonicalTipUnconfirmed
        );

        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert!(readiness.report().is_ready);

        // A park, then a restarted writer that has not yet rediscovered it,
        // keeps the remembered halt instead of the unconfirmed cause.
        control.set_writer_status(CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }),
            tip_unconfirmed: true,
            ..lifecycle_writer_status(&primary)
        });
        publisher.refresh().await;
        control.set_writer_status(unconfirmed_status);
        publisher.refresh().await;
        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::ReorgWindowExceeded { .. }
        ));
        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert!(readiness.report().is_ready);

        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// Compact serving refuses a parked writer with its halt.
    ///
    /// A parked writer keeps attesting its frozen fence, so the halt must come
    /// from canonical writer status: at bootstrap, on every refresh, and until
    /// the writer clears it.
    #[tokio::test]
    async fn publisher_refuses_a_parked_writer_with_the_typed_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, _blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let halt = zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let parked_status = CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(halt),
            ..lifecycle_writer_status(&primary)
        };
        let control = MutableCanonicalControl::new(parked_status.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });

        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let expected_halt = ReadinessCause::ReorgWindowExceeded {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let bootstrapped = readiness.report();
        assert!(!bootstrapped.is_ready);
        assert_eq!(bootstrapped.cause, expected_halt);

        control.set_writer_status(lifecycle_writer_status(&primary));
        publisher.refresh().await;
        assert!(readiness.report().is_ready);

        control.set_writer_status(parked_status);
        publisher.refresh().await;
        let parked = readiness.report();
        assert!(!parked.is_ready);
        assert_eq!(parked.cause, expected_halt);

        // A writer-status outage after the halt must keep refusing traffic,
        // never reopen it as a stale reader within the staleness ceiling.
        control.set_available(false);
        publisher.refresh().await;
        let after_outage = readiness.report();
        assert!(!after_outage.is_ready);
        assert_eq!(after_outage.cause, expected_halt);

        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// A writer that parks between a refresh's first status and the status
    /// that attests the rotated reader must leave the reader refusing with
    /// the halt, also through a later writer-status outage.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "bootstrap, the append, the park between two statuses, and the outage form one causal sequence"
    )]
    async fn rotation_attested_by_a_parked_writer_refuses_with_the_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let activations = lifecycle_activations();
        let (primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 0,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        assert!(readiness.report().is_ready);

        let second = blocks
            .get(1)
            .cloned()
            .ok_or("lifecycle fixture has no append block")?;
        let expected_fence = primary.event_fence();
        let (primary, _) = primary.commit_live_append(
            CanonicalLiveAppend::new(
                expected_fence,
                lifecycle_build_block(&second, &activations)?,
                Vec::new(),
                expected_fence.visible_tip(),
                UnixTimestampMillis::new(2_000),
            ),
            &activations,
        )?;
        // The refresh's first status is still following; the writer parks
        // before the status that attests the rotated reader.
        control.queue_writer_status(lifecycle_writer_status(&primary));
        control.set_writer_status(CanonicalWriterStatusResponse {
            reorg_window_exceeded: Some(zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }),
            ..lifecycle_writer_status(&primary)
        });
        publisher.refresh().await;
        assert_eq!(
            slot.capture().event_fence().visible_tip().height,
            BlockHeight::new(2)
        );
        let expected_halt = ReadinessCause::ReorgWindowExceeded {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        };
        let rotated = readiness.report();
        assert!(!rotated.is_ready);
        assert_eq!(rotated.cause, expected_halt);

        control.set_available(false);
        publisher.refresh().await;
        let after_outage = readiness.report();
        assert!(!after_outage.is_ready);
        assert_eq!(after_outage.cause, expected_halt);

        drop(slot);
        drop(publisher);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// A runtime started on the pre-upgrade table keeps refreshing after the
    /// chain crosses an activation the writer admitted later.
    ///
    /// `GetLightdInfo` reports the admitted upgrade as pending below its
    /// height and as the active branch from that height on.
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The scenario keeps startup, writer admission, the pending and crossing refreshes, and the served upgrade in one causal sequence."
    )]
    async fn publisher_keeps_refreshing_after_the_chain_crosses_a_later_admitted_activation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        // A pre-NU7 table whose upgrades are all active by height 2, so NU7 is
        // the only upgrade that can be pending.
        let activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            lifecycle_activations()
                .activations()
                .iter()
                .cloned()
                .map(|activation| {
                    let activation_height = if matches!(
                        activation.name.as_str(),
                        "Overwinter" | "Sapling" | "Blossom" | "Heartwood" | "Canopy"
                    ) {
                        BlockHeight::new(1)
                    } else {
                        BlockHeight::new(2)
                    };
                    NetworkUpgradeActivation {
                        activation_height,
                        ..activation
                    }
                })
                .collect(),
        )?;
        let branch_active_at = |height: u32| {
            activations
                .active_at(BlockHeight::new(height))
                .map(|activation| zinder_core::wire::encode_branch_id_hex(activation.branch_id))
                .ok_or("the construction table must have an active upgrade")
        };
        let canopy_branch_id = branch_active_at(1)?;
        let nu6_3_branch_id = branch_active_at(2)?;
        let (mut primary, blocks) =
            lifecycle_store(&temporary.path().join("canonical-primary"), &activations)?;
        let control = MutableCanonicalControl::new(lifecycle_writer_status(&primary));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn({
            let control = control.clone();
            async move {
                Server::builder()
                    .add_service(CanonicalControlServer::new(control))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            }
        });
        let serving_readiness = CompactServingReadiness::new(Readiness::default());
        let config = CompactBlockServingConfig {
            canonical_primary_path: temporary.path().join("canonical-primary"),
            canonical_secondary_root: temporary.path().join("canonical-secondaries"),
            network: Network::ZcashRegtest,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy::new(100)?,
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            catchup_interval: Duration::from_millis(1),
            convergence_timeout: Duration::from_secs(2),
            convergence_attempts: 4,
            staleness_ceiling: Duration::from_mins(1),
            lag_threshold: 4,
        };
        let (mut publisher, slot) =
            CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None).await?;
        let adapter = CompactBlocksGrpcAdapter::new(slot.clone());
        let startup = adapter
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?
            .into_inner();
        assert_eq!(startup.consensus_branch_id, canopy_branch_id);
        assert_eq!(startup.upgrade_name, "NU5");
        assert_eq!(startup.upgrade_height, 2);

        // The node is upgraded and only the writer restarts: it admits the
        // new table, which activates NU7 at height 3, as revision 2.
        let nu7_branch_id = ConsensusBranchId::new(0x7719_0ad9);
        let mut entries = activations.activations().to_vec();
        entries.push(NetworkUpgradeActivation {
            branch_id: nu7_branch_id,
            activation_height: BlockHeight::new(3),
            name: "NU7".to_owned(),
        });
        let upgraded = NetworkUpgradeActivations::new(Network::ZcashRegtest, entries)?;
        primary.admit_network_upgrade_schedule(&upgraded)?;
        for block in blocks.iter().skip(1) {
            let expected_fence = primary.event_fence();
            let (next_primary, _) = primary.commit_live_append(
                CanonicalLiveAppend::new(
                    expected_fence,
                    lifecycle_build_block(block, &upgraded)?,
                    Vec::new(),
                    expected_fence.visible_tip(),
                    UnixTimestampMillis::new(u64::from(block.height.value()) * 1_000),
                ),
                &upgraded,
            )?;
            primary = next_primary;
            control.set_writer_status(lifecycle_writer_status(&primary));
            publisher.refresh().await;
            let published_reader = slot.capture();
            assert_eq!(published_reader.event_fence(), primary.event_fence());
            assert_eq!(
                published_reader.secondary().network_upgrade_activations(),
                Some(&upgraded)
            );
            drop(published_reader);

            let info = adapter
                .get_lightd_info(Request::new(lightwalletd::Empty::default()))
                .await?
                .into_inner();
            assert_eq!(info.block_height, u64::from(block.height.value()));
            if block.height < BlockHeight::new(3) {
                assert_eq!(info.consensus_branch_id, nu6_3_branch_id);
                assert_eq!(info.upgrade_name, "NU7");
                assert_eq!(info.upgrade_height, 3);
            } else {
                assert_eq!(
                    info.consensus_branch_id,
                    zinder_core::wire::encode_branch_id_hex(nu7_branch_id)
                );
                assert_eq!(info.upgrade_name, "");
                assert_eq!(info.upgrade_height, 0);
            }
        }

        drop(adapter);
        drop(publisher);
        drop(primary);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn transport_outage_uses_stale_grace_then_fails_closed() {
        assert_eq!(
            transport_failure_cause(Some(Duration::from_secs(2))),
            ReadinessCause::ServingPairStale {
                lag_chain_epochs: 0,
                staleness_seconds: 2,
            }
        );
        assert_eq!(
            transport_failure_cause(None),
            ReadinessCause::WriterStatusUnavailable
        );
    }

    /// A canonical writer with more than one chunk of blocks, served through
    /// the compact-blocks publisher and adapter.
    struct LongRangeHarness {
        writer: zinder_testkit::CanonicalWriterFixture,
        control: MutableCanonicalControl,
        readiness: Readiness,
        publisher: CompactBlockPublisher,
        slot: CanonicalReaderSlot,
        adapter: CompactBlocksGrpcAdapter,
        shutdown: oneshot::Sender<()>,
        server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        _secondaries: tempfile::TempDir,
    }

    impl LongRangeHarness {
        async fn start(
            baseline: &zinder_testkit::CanonicalBaseline<'_>,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let activations = zinder_testkit::sample_regtest_upgrade_activations();
            let writer = zinder_testkit::CanonicalWriterFixture::publish(baseline, &activations)?;
            let control =
                MutableCanonicalControl::new(lifecycle_writer_status(writer.canonical_primary()?));
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let (shutdown, shutdown_receiver) = oneshot::channel();
            let server = tokio::spawn({
                let control = control.clone();
                async move {
                    Server::builder()
                        .add_service(CanonicalControlServer::new(control))
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(listener),
                            async move {
                                let _ = shutdown_receiver.await;
                            },
                        )
                        .await
                }
            });
            let secondaries = tempfile::tempdir()?;
            let readiness = Readiness::default();
            let serving_readiness = CompactServingReadiness::new(readiness.clone());
            let config = CompactBlockServingConfig {
                canonical_primary_path: writer.canonical_primary_path().to_path_buf(),
                canonical_secondary_root: secondaries.path().to_path_buf(),
                network: Network::ZcashRegtest,
                raw_blob_retention: RawBlobRetention::Transactions,
                reorg_policy: CanonicalReorgPolicy::new(100)?,
                resource_budget: RocksDbResourceBudget::for_local_tests(),
                catchup_interval: Duration::from_millis(1),
                convergence_timeout: Duration::from_secs(2),
                convergence_attempts: 4,
                staleness_ceiling: Duration::from_mins(1),
                lag_threshold: 0,
            };
            let (publisher, slot) =
                CompactBlockPublisher::bootstrap(config, serving_readiness, &endpoint, None)
                    .await?;
            let adapter = CompactBlocksGrpcAdapter::new(slot.clone());
            Ok(Self {
                writer,
                control,
                readiness,
                publisher,
                slot,
                adapter,
                shutdown,
                server,
                _secondaries: secondaries,
            })
        }

        /// Reports the writer's current fence and runs one publisher refresh.
        async fn refresh_at_writer_fence(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.control
                .set_writer_status(lifecycle_writer_status(self.writer.canonical_primary()?));
            self.publisher.refresh().await;
            Ok(())
        }

        fn published_height(&self) -> BlockHeight {
            self.slot.capture().event_fence().visible_tip().height
        }

        async fn stop(self) -> Result<(), Box<dyn std::error::Error>> {
            let cancel = CancellationToken::new();
            let drained = CancellationToken::new();
            let publisher_handle = self.publisher.spawn(cancel.clone(), drained.clone());
            cancel.cancel();
            drained.cancel();
            publisher_handle.await??;
            drop(self.slot);
            drop(self.adapter);
            let _ = self.shutdown.send(());
            self.server.await??;
            Ok(())
        }
    }

    fn block_range(start: u32, end: u32) -> lightwalletd::BlockRange {
        lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: u64::from(start),
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: u64::from(end),
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }
    }

    /// Reads blocks until the stream has sent `count`, or until it ends.
    async fn read_blocks<Stream>(
        stream: &mut Stream,
        blocks: &mut Vec<lightwalletd::CompactBlock>,
        count: usize,
    ) -> Result<(), tonic::Status>
    where
        Stream:
            tokio_stream::Stream<Item = Result<lightwalletd::CompactBlock, tonic::Status>> + Unpin,
    {
        while blocks.len() < count {
            let Some(message) = stream.next().await else {
                return Ok(());
            };
            blocks.push(message?);
        }
        Ok(())
    }

    /// Asserts that `blocks` are heights `1..=last_height` in order and that
    /// each names the block before it as its parent.
    fn assert_linked_from_genesis(blocks: &[lightwalletd::CompactBlock], last_height: u32) {
        assert_eq!(
            blocks.len(),
            usize::try_from(last_height).unwrap_or(usize::MAX),
            "the stream must send heights 1 through {last_height}"
        );
        for (expected_height, block) in (1_u64..).zip(blocks) {
            assert_eq!(block.height, expected_height);
        }
        for pair in blocks.windows(2) {
            let [parent, child] = pair else {
                continue;
            };
            assert_eq!(
                child.prev_hash, parent.hash,
                "block {} must link to block {}",
                child.height, parent.height
            );
        }
    }

    /// A stream that waits on a slow client holds no generation, so the
    /// publisher keeps publishing while it is open and readiness stays ready.
    #[tokio::test]
    async fn a_slow_block_range_stream_does_not_hold_a_generation_between_chunks()
    -> Result<(), Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 2_100;
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(TIP_HEIGHT + 2);
        let mut harness = LongRangeHarness::start(
            &zinder_testkit::CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP_HEIGHT)),
        )
        .await?;
        let entry_generation = Arc::downgrade(&harness.slot.capture());

        let mut stream = harness
            .adapter
            .get_block_range(Request::new(block_range(1, TIP_HEIGHT)))
            .await?
            .into_inner();
        let mut blocks = Vec::new();
        // Block 1,001 opens the second chunk, so that chunk has been read and
        // the stream now waits on this client.
        read_blocks(&mut stream, &mut blocks, 1_001).await?;

        harness.writer.append_live_block(&chain)?;
        harness.refresh_at_writer_fence().await?;
        assert_eq!(harness.published_height(), BlockHeight::new(TIP_HEIGHT + 1));
        // Only the publisher's retired slot may still hold the entry
        // generation once no chunk read does.
        let released = tokio::time::timeout(Duration::from_secs(10), async {
            while entry_generation.strong_count() > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "the open stream kept the retired generation while it waited for its client"
        );
        harness.writer.append_live_block(&chain)?;
        harness.refresh_at_writer_fence().await?;
        assert_eq!(
            harness.published_height(),
            BlockHeight::new(TIP_HEIGHT + 2),
            "a second publication follows while the stream is open"
        );
        assert_eq!(harness.readiness.report().cause.metric_label(), "ready");

        read_blocks(&mut stream, &mut blocks, usize::MAX).await?;
        assert_linked_from_genesis(&blocks, TIP_HEIGHT);
        drop(stream);
        harness.stop().await
    }

    /// A descending stream links each later chunk to the parent of the last
    /// block it sent, so it crosses chunk edges and a publication without
    /// ending.
    #[tokio::test]
    async fn a_descending_block_range_stream_crosses_chunks_and_publications()
    -> Result<(), Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 2_100;
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(TIP_HEIGHT + 1);
        let mut harness = LongRangeHarness::start(
            &zinder_testkit::CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP_HEIGHT)),
        )
        .await?;

        let mut stream = harness
            .adapter
            .get_block_range(Request::new(block_range(TIP_HEIGHT, 1)))
            .await?
            .into_inner();
        let mut blocks = Vec::new();
        // The chunks are 2,100 to 1,101, 1,100 to 101, and 100 to 1. Block
        // 1,100 opens the second chunk, so it is read before the publication,
        // and the third waits for this client and is read after it.
        read_blocks(&mut stream, &mut blocks, 1_001).await?;
        harness.writer.append_live_block(&chain)?;
        harness.refresh_at_writer_fence().await?;
        assert_eq!(harness.published_height(), BlockHeight::new(TIP_HEIGHT + 1));

        read_blocks(&mut stream, &mut blocks, usize::MAX).await?;
        assert_eq!(
            blocks.first().map(|block| block.height),
            Some(u64::from(TIP_HEIGHT))
        );
        blocks.reverse();
        assert_linked_from_genesis(&blocks, TIP_HEIGHT);
        drop(stream);
        harness.stop().await
    }

    /// What a stream over heights 1 through 2,050 sent across a reorg.
    ///
    /// The reorg is a live replacement from `fork_height` through
    /// `replacement_tip_height`, published while the stream waits on its
    /// client.
    struct ReorgedStream {
        blocks: Vec<lightwalletd::CompactBlock>,
        ending: Option<tonic::Status>,
    }

    async fn stream_across_reorg(
        fork_height: u32,
        replacement_tip_height: u32,
    ) -> Result<ReorgedStream, Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 2_050;
        let mut chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(TIP_HEIGHT);
        for height in fork_height..=TIP_HEIGHT {
            let displaced = chain
                .block_at(BlockHeight::new(height))
                .cloned()
                .ok_or("fixture has no block to displace")?;
            let mut transaction_id = [0x5a; 32];
            transaction_id[..4].copy_from_slice(&height.to_le_bytes());
            chain = chain.with_transaction_rows(zinder_testkit::FixtureTransactionRows::coinbase(
                zinder_core::TransactionId::from_bytes(transaction_id),
                displaced.height,
                displaced.hash,
                format!("raw-displaced-coinbase-{height}").into_bytes(),
            ));
        }
        let replacement_chain = chain
            .fork_at(BlockHeight::new(fork_height))?
            .extend_blocks(replacement_tip_height - fork_height + 1);
        let mut harness = LongRangeHarness::start(
            &zinder_testkit::CanonicalBaseline::new(&chain)
                .with_settled_tip(BlockHeight::new(TIP_HEIGHT - 90)),
        )
        .await?;
        let sent_epoch = harness.slot.capture().chain_epoch().id;

        let mut stream = harness
            .adapter
            .get_block_range(Request::new(block_range(1, TIP_HEIGHT)))
            .await?
            .into_inner();
        let mut blocks = Vec::new();
        // The second chunk is read before the replacement and the third
        // after it.
        read_blocks(&mut stream, &mut blocks, 1_001).await?;
        harness
            .writer
            .replace_live_blocks(&replacement_chain, BlockHeight::new(fork_height))?;
        harness.refresh_at_writer_fence().await?;
        assert_ne!(harness.slot.capture().chain_epoch().id, sent_epoch);

        let ending = read_blocks(&mut stream, &mut blocks, usize::MAX)
            .await
            .err();
        drop(stream);
        harness.stop().await?;
        Ok(ReorgedStream { blocks, ending })
    }

    /// A reorg that replaces a block the stream already sent ends the stream
    /// with `ABORTED` after the blocks it sent, as lightwalletd does.
    #[tokio::test]
    async fn a_block_range_stream_ends_aborted_when_a_sent_block_is_replaced()
    -> Result<(), Box<dyn std::error::Error>> {
        let reorged = stream_across_reorg(1_990, 2_050).await?;
        assert_linked_from_genesis(&reorged.blocks, 2_000);
        assert_eq!(
            reorged.ending.as_ref().map(tonic::Status::code),
            Some(Code::Aborted),
            "{:?}",
            reorged.ending
        );
        Ok(())
    }

    /// A later chunk whose read does not continue the sent chain still
    /// records its read, as a first chunk and every wallet-serving read do,
    /// and records no conversion or send, because neither ran.
    #[tokio::test]
    async fn a_later_chunk_that_ends_the_stream_records_its_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let metrics_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = metrics_recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);

        let reorged = stream_across_reorg(1_990, 2_050).await?;
        assert_eq!(
            reorged.ending.as_ref().map(tonic::Status::code),
            Some(Code::Aborted)
        );

        // Chunks 1 and 2 are sent; chunk 3 is read and fails the hash link.
        let rendered = metrics_handle.render();
        assert_eq!(block_range_stage_samples(&rendered, "read"), Some(3));
        assert_eq!(block_range_stage_samples(&rendered, "convert"), Some(2));
        assert_eq!(block_range_stage_samples(&rendered, "send"), Some(1));
        Ok(())
    }

    /// A reorg that leaves the visible tip below a height still to send ends
    /// the stream with `OUT_OF_RANGE`, not the `NOT_FOUND` of a read above the
    /// tip.
    #[tokio::test]
    async fn a_block_range_stream_ends_out_of_range_when_the_tip_falls_below_the_range()
    -> Result<(), Box<dyn std::error::Error>> {
        let reorged = stream_across_reorg(2_020, 2_030).await?;
        assert_linked_from_genesis(&reorged.blocks, 2_000);
        assert_eq!(
            reorged.ending.as_ref().map(tonic::Status::code),
            Some(Code::OutOfRange),
            "{:?}",
            reorged.ending
        );
        Ok(())
    }

    /// Compact-blocks serving refuses a block-range stream that waited past
    /// its admission wait with `UNAVAILABLE`, after it rejects a malformed
    /// request.
    ///
    /// A stream wider than one chunk keeps its slot while its reader runs.
    /// Once the client drops the stream, the slot returns when the reader
    /// stops, and a waiting request takes it.
    #[tokio::test]
    async fn block_range_streams_past_the_limit_are_refused_until_a_slot_returns()
    -> Result<(), Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 2_100;
        let metrics_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = metrics_recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(TIP_HEIGHT);
        let harness = LongRangeHarness::start(
            &zinder_testkit::CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP_HEIGHT)),
        )
        .await?;
        let adapter = harness
            .adapter
            .clone()
            .with_max_concurrent_block_range_streams(std::num::NonZeroU32::MIN);
        let impatient = impatient_clone(&adapter);

        let mut stream = adapter
            .get_block_range(Request::new(block_range(1, TIP_HEIGHT)))
            .await?
            .into_inner();
        let mut blocks = Vec::new();
        read_blocks(&mut stream, &mut blocks, 1).await?;
        let refused = impatient
            .get_block_range(Request::new(block_range(1, 4)))
            .await;
        assert_eq!(
            refused.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        // A malformed request is rejected before it takes a slot.
        let malformed = adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                pool_types: vec![999],
                ..block_range(1, 4)
            }))
            .await;
        assert_eq!(
            malformed.err().map(|status| status.code()),
            Some(Code::InvalidArgument)
        );

        drop(stream);
        // The test runtime has one thread, so the reader of later chunks has
        // not run since the stream was dropped. It still holds the slot, so
        // the next request waits until the reader stops.
        assert_eq!(
            open_block_range_streams(&metrics_handle.render()),
            Some(1),
            "the reader of a dropped stream must keep its slot until it stops"
        );
        let next = adapter.get_block_range(Request::new(block_range(1, 4)));
        let admitted = tokio::time::timeout(Duration::from_secs(10), next)
            .await??
            .into_inner();
        let mut admitted = admitted;
        let mut short_blocks = Vec::new();
        read_blocks(&mut admitted, &mut short_blocks, usize::MAX).await?;
        assert_linked_from_genesis(&short_blocks, 4);
        drop(admitted);
        harness.stop().await
    }

    /// Returns a clone of `adapter` that shares its block-range slots but
    /// waits only briefly for one, so a test can observe a refusal quickly.
    fn impatient_clone(adapter: &CompactBlocksGrpcAdapter) -> CompactBlocksGrpcAdapter {
        adapter
            .clone()
            .with_block_range_admission_wait(Duration::from_millis(50))
    }

    /// Polls `call` once, so a test can cancel it where it first waits.
    fn poll_once<Call: Future + Unpin>(call: &mut Call) -> std::task::Poll<Call::Output> {
        std::pin::Pin::new(call).poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    /// A request cancelled during its first chunk read keeps its slot until
    /// the read has finished.
    ///
    /// The runtime has one blocking thread and the test holds it, so the
    /// first chunk's read cannot finish until the test lets it run. The slot
    /// returns once the read has finished.
    #[test]
    fn a_request_cancelled_during_its_first_read_keeps_its_slot_until_the_read_ends()
    -> Result<(), Box<dyn std::error::Error>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(cancel_during_a_gated_first_read())
    }

    async fn cancel_during_a_gated_first_read() -> Result<(), Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 10;
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(TIP_HEIGHT);
        let harness = LongRangeHarness::start(
            &zinder_testkit::CanonicalBaseline::new(&chain).through(BlockHeight::new(TIP_HEIGHT)),
        )
        .await?;
        let adapter = harness
            .adapter
            .clone()
            .with_max_concurrent_block_range_streams(std::num::NonZeroU32::MIN);
        let generation = Arc::downgrade(&harness.slot.capture());

        let (gate_held, gate_held_receiver) = oneshot::channel();
        let (release_gate, gate) = oneshot::channel::<()>();
        let gate_thread = tokio::task::spawn_blocking(move || {
            let _ = gate_held.send(());
            let _ = gate.blocking_recv();
        });
        gate_held_receiver.await?;

        // One poll takes the slot and starts the first chunk's read, which
        // waits for the blocking thread. Dropping the call cancels it there.
        let mut cancelled = adapter.get_block_range(Request::new(block_range(1, TIP_HEIGHT)));
        assert!(poll_once(&mut cancelled).is_pending());
        drop(cancelled);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        let mut waiting = adapter.get_block_range(Request::new(block_range(1, 4)));
        assert!(
            poll_once(&mut waiting).is_pending(),
            "a request cancelled during its first read must keep its slot while the read runs"
        );
        assert_eq!(
            generation.strong_count(),
            2,
            "the cancelled request's first read still holds its generation"
        );

        let _ = release_gate.send(());
        gate_thread.await?;
        let mut admitted = tokio::time::timeout(Duration::from_secs(10), waiting)
            .await??
            .into_inner();
        assert_eq!(
            generation.strong_count(),
            1,
            "the slot returns only after the cancelled request's first read has finished"
        );
        let mut blocks = Vec::new();
        read_blocks(&mut admitted, &mut blocks, usize::MAX).await?;
        assert_linked_from_genesis(&blocks, 4);
        drop(admitted);
        harness.stop().await
    }

    /// `GetLightdInfo`, `GetLatestBlock`, and `GetBlock` read the canonical
    /// store on the blocking pool, never on a reactor thread.
    ///
    /// The runtime has one blocking thread and the test holds it, so each
    /// call waits at its first storage read until the test releases the
    /// thread.
    #[test]
    fn unary_block_methods_read_storage_on_the_blocking_pool()
    -> Result<(), Box<dyn std::error::Error>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(call_unary_block_methods_while_the_blocking_pool_is_held())
    }

    async fn call_unary_block_methods_while_the_blocking_pool_is_held()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let (reader, _fixture) = large_block_reader(&chain)?;
        let tip_hash =
            zinder_core::wire::encode_internal_block_hash(reader.chain_epoch().visible_tip_hash)
                .to_vec();
        let adapter = adapter_serving(&reader);

        let (gate_held, gate_held_receiver) = oneshot::channel();
        let (release_gate, gate) = oneshot::channel::<()>();
        let gate_thread = tokio::task::spawn_blocking(move || {
            let _ = gate_held.send(());
            let _ = gate.blocking_recv();
        });
        gate_held_receiver.await?;

        let mut lightd_info = adapter.get_lightd_info(Request::new(lightwalletd::Empty::default()));
        let mut latest_block =
            adapter.get_latest_block(Request::new(lightwalletd::ChainSpec::default()));
        let mut block_at_height = adapter.get_block(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }));
        let mut block_by_hash = adapter.get_block(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: tip_hash,
        }));
        assert!(
            poll_once(&mut lightd_info).is_pending(),
            "GetLightdInfo must read the tip header on the blocking pool"
        );
        assert!(
            poll_once(&mut latest_block).is_pending(),
            "GetLatestBlock must read the tip header on the blocking pool"
        );
        assert!(
            poll_once(&mut block_at_height).is_pending(),
            "GetBlock must read the block on the blocking pool"
        );
        assert!(
            poll_once(&mut block_by_hash).is_pending(),
            "GetBlock must resolve a hash on the blocking pool"
        );

        let _ = release_gate.send(());
        gate_thread.await?;
        assert_eq!(lightd_info.await?.into_inner().block_height, 4);
        assert_eq!(latest_block.await?.into_inner().height, 4);
        assert_eq!(block_at_height.await?.into_inner().height, 2);
        assert_eq!(block_by_hash.await?.into_inner().height, 4);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_capture_pins_old_generation_until_request_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut first =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let mut second =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (first_reader, first_wallet) = first.take_readers()?;
        let (second_reader, second_wallet) = second.take_readers()?;
        drop(first_wallet);
        drop(second_wallet);

        let slot = CanonicalReaderSlot::new(Arc::new(RocksDbCanonicalReader::new(first_reader)?));
        let pinned = slot.capture();
        let retired = slot.publish(Arc::new(RocksDbCanonicalReader::new(second_reader)?));
        assert_eq!(
            retired.event_fence().visible_tip(),
            pinned.event_fence().visible_tip()
        );
        assert!(Arc::strong_count(&retired) >= 2);
        drop(pinned);
        assert_eq!(Arc::strong_count(&retired), 1);
        Ok(())
    }

    /// Compact-block serving keeps the lightwalletd range status contract:
    /// `OUT_OF_RANGE` above the visible tip and `NOT_FOUND` for an unretained
    /// start, never a retryable `UNAVAILABLE` storage outage.
    #[tokio::test]
    async fn get_block_range_outside_retained_history_keeps_lightwalletd_codes()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(3);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
            RocksDbCanonicalReader::new(canonical)?,
        )));

        let cases = [
            ("start above visible tip", 4_u64, 5_u64, Code::OutOfRange),
            ("start at unretained height 0", 0, 1, Code::NotFound),
            ("only unretained height 0", 0, 0, Code::NotFound),
            ("descending to unretained height 0", 1, 0, Code::NotFound),
        ];
        let mut mismatches = Vec::new();
        for (label, start_height, end_height, expected_code) in cases {
            let range = lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: start_height,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: end_height,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            };
            let observed = match adapter.get_block_range(Request::new(range)).await {
                Err(status) => Some(status),
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let mut first_error = None;
                    while let Some(message) = stream.next().await {
                        if let Err(status) = message {
                            first_error = Some(status);
                            break;
                        }
                    }
                    first_error
                }
            };
            match observed {
                Some(status) if status.code() == expected_code => {}
                Some(status) => mismatches.push(format!(
                    "{label} ({start_height}..={end_height}): expected {expected_code:?}, observed {:?} \"{}\"",
                    status.code(),
                    status.message(),
                )),
                None => mismatches.push(format!(
                    "{label} ({start_height}..={end_height}): expected {expected_code:?}, observed a complete stream"
                )),
            }
        }

        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(mismatches.join("\n").into())
        }
    }

    /// A first chunk whose read fails records the read it attempted and no
    /// conversion, because nothing was converted.
    #[tokio::test]
    async fn a_failed_first_read_records_no_convert_stage() -> Result<(), Box<dyn std::error::Error>>
    {
        let metrics_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = metrics_recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
        let (reader, _fixture) = large_block_reader(&large_block_chain(None)?)?;
        let adapter = adapter_serving(&reader);

        let outcome = range_parity_blocks(&adapter, 0, 1, Vec::new()).await;
        assert!(outcome.is_err(), "unretained height 0 must fail the read");

        let rendered = metrics_handle.render();
        assert_eq!(block_range_stage_samples(&rendered, "read"), Some(1));
        assert_eq!(block_range_stage_samples(&rendered, "convert"), None);
        Ok(())
    }

    const RANGE_PARITY_TIP: u32 = 4;

    fn range_parity_transaction_id(height: u32, index: u64) -> zinder_core::TransactionId {
        let mut bytes = [0_u8; 32];
        bytes[0] = u8::try_from(height).unwrap_or(u8::MAX);
        bytes[1] = u8::try_from(index).unwrap_or(u8::MAX);
        zinder_core::TransactionId::from_bytes(bytes)
    }

    fn range_parity_transparent_only(height: u32, index: u64) -> zinder_core::CompactTransaction {
        zinder_core::CompactTransaction {
            index,
            transaction_id: range_parity_transaction_id(height, index),
            data: zinder_core::CompactTransactionData {
                transparent_outputs: vec![zinder_core::CompactTransparentOutput {
                    value_zat: 625_000_000,
                    script_pub_key: vec![0x76, 0xa9],
                }],
                ..zinder_core::CompactTransactionData::default()
            },
        }
    }

    fn range_parity_sapling(height: u32, index: u64) -> zinder_core::CompactTransaction {
        zinder_core::CompactTransaction {
            index,
            transaction_id: range_parity_transaction_id(height, index),
            data: zinder_core::CompactTransactionData {
                sapling_spends: vec![zinder_core::CompactSaplingSpend {
                    nullifier: [u8::try_from(index).unwrap_or(u8::MAX); 32],
                }],
                ..zinder_core::CompactTransactionData::default()
            },
        }
    }

    /// Builds a four-block regtest chain mixing transparent-only and shielded
    /// transactions.
    ///
    /// The default shielded selection empties the transparent-only ones, so
    /// the served range exercises transaction omission and position
    /// preservation.
    fn range_parity_adapter() -> Result<CompactBlocksGrpcAdapter, Box<dyn std::error::Error>> {
        let mut chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .extend_blocks(RANGE_PARITY_TIP);
        let transactions_by_height = [
            vec![
                range_parity_transparent_only(1, 0),
                range_parity_sapling(1, 1),
            ],
            vec![range_parity_transparent_only(2, 0)],
            vec![
                range_parity_transparent_only(3, 0),
                range_parity_sapling(3, 1),
                range_parity_transparent_only(3, 2),
                range_parity_sapling(3, 3),
            ],
            Vec::new(),
        ];
        for (offset, transactions) in transactions_by_height.into_iter().enumerate() {
            let height = BlockHeight::new(1 + u32::try_from(offset)?);
            let block = chain
                .block_at(height)
                .ok_or("fixture block missing")?
                .clone();
            let parent_hash = if height == BlockHeight::new(1) {
                Network::ZcashRegtest.genesis_hash()
            } else {
                block.parent_hash
            };
            for transaction in &transactions {
                let tx_index = u32::try_from(transaction.index)?;
                let mut rows = zinder_testkit::FixtureTransactionRows::from_raw_transaction(
                    transaction.transaction_id,
                    block.height,
                    block.hash,
                    tx_index,
                    format!("range-parity-{}-{tx_index}", block.height.value()).into_bytes(),
                );
                rows.facts.public_facts.counts = zinder_core::TransactionComponentCounts {
                    sapling_spend_count: u32::try_from(transaction.data.sapling_spends.len())?,
                    transparent_output_count: u32::try_from(
                        transaction.data.transparent_outputs.len(),
                    )?,
                    ..zinder_core::TransactionComponentCounts::EMPTY
                };
                chain = chain.with_transaction_rows(rows);
            }
            chain = chain.with_compact_block_artifact(zinder_core::CompactBlockArtifact::new(
                BlockId::new(block.height, block.hash),
                parent_hash,
                block.block_time_seconds,
                transactions,
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            )?);
        }
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        Ok(CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
        )))
    }

    async fn range_parity_blocks(
        adapter: &CompactBlocksGrpcAdapter,
        start_height: u64,
        end_height: u64,
        pool_types: Vec<i32>,
    ) -> Result<Vec<lightwalletd::CompactBlock>, Box<dyn std::error::Error>> {
        let mut stream = adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: start_height,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: end_height,
                    hash: Vec::new(),
                }),
                pool_types,
            }))
            .await?
            .into_inner();
        let mut blocks = Vec::new();
        while let Some(block) = stream.next().await {
            blocks.push(block?);
        }
        Ok(blocks)
    }

    /// Sapling spends in each block of the large-block range test, about
    /// 100 KB per stored block, so one byte-capped chunk keeps about 20.
    const LARGE_BLOCK_SAPLING_SPENDS: u32 = 3_000;

    /// Highest height of the large-block range test: about 5 MB of stored
    /// blocks, more than two byte-capped chunks.
    const LARGE_BLOCK_TIP: u32 = 50;

    /// A regtest chain whose every block carries one transaction of
    /// [`LARGE_BLOCK_SAPLING_SPENDS`] Sapling spends, on a new branch from
    /// `fork_height` on when one is given.
    fn large_block_chain(
        fork_height: Option<u32>,
    ) -> Result<zinder_testkit::ChainFixture, Box<dyn std::error::Error>> {
        let mut chain =
            zinder_testkit::ChainFixture::new(Network::ZcashRegtest).extend_blocks(LARGE_BLOCK_TIP);
        if let Some(fork_height) = fork_height {
            chain = chain
                .fork_at(BlockHeight::new(fork_height))?
                .extend_blocks(LARGE_BLOCK_TIP - fork_height + 1);
        }
        for height in 1..=LARGE_BLOCK_TIP {
            let spends = (0..LARGE_BLOCK_SAPLING_SPENDS)
                .map(|spend| {
                    let mut nullifier = [0_u8; 32];
                    nullifier[..4].copy_from_slice(&height.to_le_bytes());
                    nullifier[4..8].copy_from_slice(&spend.to_le_bytes());
                    zinder_core::CompactSaplingSpend { nullifier }
                })
                .collect();
            chain = with_one_transaction_at(
                chain,
                height,
                zinder_core::CompactTransactionData {
                    sapling_spends: spends,
                    ..zinder_core::CompactTransactionData::default()
                },
            )?;
        }
        Ok(chain)
    }

    /// Replaces the compact block at `height` of `chain` with one that holds
    /// a single transaction of `data`.
    fn with_one_transaction_at(
        chain: zinder_testkit::ChainFixture,
        height: u32,
        data: zinder_core::CompactTransactionData,
    ) -> Result<zinder_testkit::ChainFixture, Box<dyn std::error::Error>> {
        let block = chain
            .block_at(BlockHeight::new(height))
            .ok_or("fixture block missing")?
            .clone();
        let parent_hash = if height == 1 {
            Network::ZcashRegtest.genesis_hash()
        } else {
            block.parent_hash
        };
        let mut rows = zinder_testkit::FixtureTransactionRows::from_raw_transaction(
            range_parity_transaction_id(height, 0),
            block.height,
            block.hash,
            0,
            format!("one-transaction-block-{height}").into_bytes(),
        );
        rows.facts.public_facts.counts = zinder_core::TransactionComponentCounts {
            sapling_spend_count: u32::try_from(data.sapling_spends.len())?,
            ..zinder_core::TransactionComponentCounts::EMPTY
        };
        let transaction = zinder_core::CompactTransaction {
            index: 0,
            transaction_id: range_parity_transaction_id(height, 0),
            data,
        };
        Ok(chain
            .with_transaction_rows(rows)
            .with_compact_block_artifact(zinder_core::CompactBlockArtifact::new(
                BlockId::new(block.height, block.hash),
                parent_hash,
                block.block_time_seconds,
                vec![transaction],
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            )?))
    }

    /// Opens the canonical reader of `chain`.
    ///
    /// The returned fixture owns the store's directory and must outlive the
    /// reader.
    fn large_block_reader(
        chain: &zinder_testkit::ChainFixture,
    ) -> Result<
        (
            Arc<RocksDbCanonicalReader>,
            zinder_testkit::WalletServingStoreFixture,
        ),
        Box<dyn std::error::Error>,
    > {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(chain, &activations)?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        Ok((Arc::new(RocksDbCanonicalReader::new(canonical)?), fixture))
    }

    fn adapter_serving(reader: &Arc<RocksDbCanonicalReader>) -> CompactBlocksGrpcAdapter {
        CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::clone(reader)))
    }

    /// Returns how many blocks of `1..=LARGE_BLOCK_TIP` one byte-capped chunk
    /// read of `reader` keeps.
    fn first_chunk_blocks(
        reader: &RocksDbCanonicalReader,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let first_chunk = reader.secondary().compact_blocks_in_range_up_to(
            zinder_core::BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(LARGE_BLOCK_TIP),
            ),
            zinder_store::HeightOrder::Ascending,
            zinder_compat_lightwalletd::BLOCK_RANGE_CHUNK_MAX_BYTES,
        )?;
        Ok(u32::try_from(first_chunk.len())?)
    }

    /// Compact-blocks serving sends a range of large blocks unchanged.
    ///
    /// The range spans several byte-capped chunks, the first included, and
    /// the stream sends exactly the blocks that one-block requests return,
    /// each linked to the block before it.
    #[tokio::test]
    async fn get_block_range_sends_large_blocks_in_byte_capped_chunks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, _fixture) = large_block_reader(&large_block_chain(None)?)?;
        let stored_bytes: usize = reader
            .secondary()
            .compact_blocks_in_range(zinder_core::BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(LARGE_BLOCK_TIP),
            ))?
            .iter()
            .map(|block| zinder_store::encode_compact_block_artifact(block).len())
            .sum();
        assert!(
            stored_bytes > 2 * zinder_compat_lightwalletd::BLOCK_RANGE_CHUNK_MAX_BYTES,
            "the range must span more than two chunks: {stored_bytes} bytes"
        );
        let adapter = adapter_serving(&reader);

        let mut one_block_reads = Vec::new();
        for height in 1..=u64::from(LARGE_BLOCK_TIP) {
            one_block_reads
                .extend(range_parity_blocks(&adapter, height, height, Vec::new()).await?);
        }
        let ascending =
            range_parity_blocks(&adapter, 1, u64::from(LARGE_BLOCK_TIP), Vec::new()).await?;
        assert_linked_from_genesis(&ascending, LARGE_BLOCK_TIP);
        assert!(
            ascending == one_block_reads,
            "byte-capped chunks must send the blocks that one-block reads return"
        );
        let mut descending =
            range_parity_blocks(&adapter, u64::from(LARGE_BLOCK_TIP), 1, Vec::new()).await?;
        descending.reverse();
        assert!(
            descending == one_block_reads,
            "a descending stream must send the one-block reads in reverse"
        );
        Ok(())
    }

    /// Sapling spends in each block of the read-ahead test, about 1.1 MB per
    /// stored block, so each byte-capped chunk keeps one block.
    const ONE_BLOCK_CHUNK_SAPLING_SPENDS: u32 = 34_000;

    /// Waits until `condition` holds, polling while other tasks run.
    async fn wait_until(
        condition: impl Fn() -> bool + Send + Sync,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }

    /// A stream of one-block chunks reads two chunks before its client takes
    /// a block.
    ///
    /// The response holds the first chunk and the stream's reader queues the
    /// second. The reader reads on once the client has taken the first.
    #[tokio::test]
    async fn an_unread_stream_of_one_block_chunks_reads_two_chunks()
    -> Result<(), Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 22;
        let metrics_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = metrics_recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
        let mut chain =
            zinder_testkit::ChainFixture::new(Network::ZcashRegtest).extend_blocks(TIP_HEIGHT);
        for height in 1..=TIP_HEIGHT {
            let spends = (0..ONE_BLOCK_CHUNK_SAPLING_SPENDS)
                .map(|spend| {
                    let mut nullifier = [0_u8; 32];
                    nullifier[..4].copy_from_slice(&height.to_le_bytes());
                    nullifier[4..8].copy_from_slice(&spend.to_le_bytes());
                    zinder_core::CompactSaplingSpend { nullifier }
                })
                .collect();
            chain = with_one_transaction_at(
                chain,
                height,
                zinder_core::CompactTransactionData {
                    sapling_spends: spends,
                    ..zinder_core::CompactTransactionData::default()
                },
            )?;
        }
        let (reader, _fixture) = large_block_reader(&chain)?;
        let first_chunk = reader.secondary().compact_blocks_in_range_up_to(
            zinder_core::BlockHeightRange::inclusive(
                BlockHeight::new(1),
                BlockHeight::new(TIP_HEIGHT),
            ),
            zinder_store::HeightOrder::Ascending,
            zinder_compat_lightwalletd::BLOCK_RANGE_CHUNK_MAX_BYTES,
        )?;
        assert_eq!(first_chunk.len(), 1, "each chunk must keep one block");
        let adapter = adapter_serving(&reader);
        let chunk_reads =
            || block_range_stage_samples(&metrics_handle.render(), "read").unwrap_or(0);

        let mut stream = adapter
            .get_block_range(Request::new(block_range(1, TIP_HEIGHT)))
            .await?
            .into_inner();
        wait_until(|| chunk_reads() >= 2).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            chunk_reads(),
            2,
            "a stream must read no chunk past its second before its client takes a block"
        );

        let mut blocks = Vec::new();
        read_blocks(&mut stream, &mut blocks, usize::MAX).await?;
        assert_linked_from_genesis(&blocks, TIP_HEIGHT);
        assert_eq!(chunk_reads(), u64::from(TIP_HEIGHT));
        Ok(())
    }

    /// A compact-blocks adapter served on a local listener, and a generated
    /// client connected to it.
    struct ServedAdapter {
        client: CompactTxStreamerClient<tonic::transport::Channel>,
        shutdown: oneshot::Sender<()>,
        server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl ServedAdapter {
        async fn start(
            adapter: CompactBlocksGrpcAdapter,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let (shutdown, shutdown_receiver) = oneshot::channel();
            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(adapter.into_server())
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_receiver.await;
                    })
                    .await
            });
            let client = CompactTxStreamerClient::connect(endpoint).await?;
            Ok(Self {
                client,
                shutdown,
                server,
            })
        }

        async fn stop(self) -> Result<(), Box<dyn std::error::Error>> {
            drop(self.client);
            let _ = self.shutdown.send(());
            self.server.await??;
            Ok(())
        }
    }

    /// Serves heights `1..=tip_height`, where the block at `unconvertible`
    /// carries a fee that lightwalletd's `uint32` fee field cannot hold.
    async fn serve_a_chain_with_an_unconvertible_block(
        tip_height: u32,
        unconvertible: u32,
    ) -> Result<
        (ServedAdapter, zinder_testkit::WalletServingStoreFixture),
        Box<dyn std::error::Error>,
    > {
        let chain = with_one_transaction_at(
            zinder_testkit::ChainFixture::new(Network::ZcashRegtest).extend_blocks(tip_height),
            unconvertible,
            zinder_core::CompactTransactionData {
                fee_zat: Some(u64::from(u32::MAX) + 1),
                sapling_spends: vec![zinder_core::CompactSaplingSpend { nullifier: [7; 32] }],
                ..zinder_core::CompactTransactionData::default()
            },
        )?;
        let (reader, fixture) = large_block_reader(&chain)?;
        Ok((
            ServedAdapter::start(adapter_serving(&reader)).await?,
            fixture,
        ))
    }

    /// A first chunk with a block that cannot be converted fails the call
    /// before the stream opens, so the client receives no block of it.
    #[tokio::test]
    async fn an_unconvertible_block_in_the_first_chunk_fails_the_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut served, _fixture) = serve_a_chain_with_an_unconvertible_block(3, 2).await?;

        let refusal = served
            .client
            .get_block_range(block_range(1, 3))
            .await
            .err()
            .ok_or("the call opened a stream over an unconvertible block")?;

        assert_eq!(refusal.code(), Code::DataLoss, "{refusal:?}");
        served.stop().await
    }

    /// A later chunk with a block that cannot be converted ends the stream
    /// after the chunks before it, and sends no block of that chunk.
    #[tokio::test]
    async fn an_unconvertible_block_in_a_later_chunk_sends_none_of_that_chunk()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut served, _fixture) =
            serve_a_chain_with_an_unconvertible_block(1_005, 1_003).await?;

        let mut stream = served
            .client
            .get_block_range(block_range(1, 1_005))
            .await?
            .into_inner();
        let mut received = Vec::new();
        let ending = loop {
            match stream.message().await {
                Ok(Some(block)) => received.push(block),
                Ok(None) => break None,
                Err(status) => break Some(status),
            }
        };

        assert_eq!(ending.map(|status| status.code()), Some(Code::DataLoss));
        assert_linked_from_genesis(&received, 1_000);
        drop(stream);
        served.stop().await
    }

    /// Returns how many block-range streams `rendered` reports open.
    fn open_block_range_streams(rendered: &str) -> Option<u64> {
        rendered.lines().find_map(|line| {
            line.strip_prefix("zinder_compat_block_range_streams_open ")?
                .parse::<u64>()
                .ok()
        })
    }

    /// Returns how many samples `rendered` holds for the block-range `stage`.
    fn block_range_stage_samples(rendered: &str, stage: &str) -> Option<u64> {
        let series =
            format!("zinder_compat_block_range_stage_duration_seconds_count{{stage=\"{stage}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(series.as_str())?.parse::<u64>().ok())
    }

    /// Returns the seconds `rendered` records for the block-range `stage`.
    fn block_range_stage_seconds(rendered: &str, stage: &str) -> Option<f64> {
        let series =
            format!("zinder_compat_block_range_stage_duration_seconds_sum{{stage=\"{stage}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(series.as_str())?.parse::<f64>().ok())
    }

    fn one_blocking_thread_runtime() -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
    }

    /// A blocking task that keeps the blocking thread running it until it is
    /// released.
    struct BlockingGate {
        release: oneshot::Sender<()>,
        task: tokio::task::JoinHandle<()>,
    }

    impl BlockingGate {
        /// Queues a gate behind every blocking task already queued.
        fn queue() -> (Self, oneshot::Receiver<()>) {
            let (running, running_receiver) = oneshot::channel();
            let (release, released) = oneshot::channel::<()>();
            let task = tokio::task::spawn_blocking(move || {
                let _ = running.send(());
                let _ = released.blocking_recv();
            });
            (Self { release, task }, running_receiver)
        }

        /// Takes the only blocking thread of the runtime.
        async fn hold() -> Result<Self, Box<dyn std::error::Error>> {
            let (gate, running) = Self::queue();
            running.await?;
            Ok(gate)
        }

        async fn release(self) -> Result<(), Box<dyn std::error::Error>> {
            self.release
                .send(())
                .map_err(|()| "the blocking gate stopped waiting")?;
            self.task.await?;
            Ok(())
        }
    }

    /// A block requested by hash is resolved and read in one storage read,
    /// so in one hop to the blocking pool.
    ///
    /// The runtime has one blocking thread. A gate queued behind the
    /// request's read keeps it, so a second read would wait behind the gate.
    #[test]
    fn a_block_requested_by_hash_takes_one_blocking_hop() -> Result<(), Box<dyn std::error::Error>>
    {
        one_blocking_thread_runtime()?.block_on(request_a_block_by_hash_ahead_of_a_gate())
    }

    async fn request_a_block_by_hash_ahead_of_a_gate() -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(3);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
            RocksDbCanonicalReader::new(canonical)?,
        )));
        let by_height = adapter
            .get_block(Request::new(lightwalletd::BlockId {
                height: 2,
                hash: Vec::new(),
            }))
            .await?
            .into_inner();

        let held = BlockingGate::hold().await?;
        let mut by_hash = adapter.get_block(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: by_height.hash.clone(),
        }));
        assert!(
            poll_once(&mut by_hash).is_pending(),
            "the request's read must wait for the held blocking thread"
        );
        let (queued, _running) = BlockingGate::queue();
        held.release().await?;
        let block = tokio::time::timeout(Duration::from_secs(5), by_hash)
            .await
            .map_err(|_| "the request made a second read, which waits behind the gate")??
            .into_inner();
        assert_eq!(block.encode_to_vec(), by_height.encode_to_vec());
        queued.release().await
    }

    /// Streams blocks `1..=2_100`, more than one chunk, while the test holds
    /// the only blocking thread from before the second chunk's read is queued
    /// until `hold` has passed.
    ///
    /// Returns how many holders the generation the stream started at had
    /// while that read waited.
    async fn stream_with_a_queued_later_chunk(
        hold: Duration,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        const TIP_HEIGHT: u32 = 2_100;
        let chain = zinder_testkit::ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(RawBlobRetention::Transactions)
            .extend_blocks(TIP_HEIGHT);
        let harness =
            LongRangeHarness::start(&zinder_testkit::CanonicalBaseline::new(&chain)).await?;
        let entry_generation = Arc::downgrade(&harness.slot.capture());

        let mut stream = harness
            .adapter
            .get_block_range(Request::new(block_range(1, TIP_HEIGHT)))
            .await?
            .into_inner();
        // Taken before the stream's task runs, so its second chunk's read
        // queues behind the gate.
        let gate = BlockingGate::hold().await?;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let generation_holders = entry_generation.strong_count();
        tokio::time::sleep(hold).await;
        gate.release().await?;

        let mut blocks = Vec::new();
        read_blocks(&mut stream, &mut blocks, usize::MAX).await?;
        assert_linked_from_genesis(&blocks, TIP_HEIGHT);
        drop(stream);
        harness.stop().await?;
        Ok(generation_holders)
    }

    /// A later chunk whose read waits for a blocking thread holds no
    /// generation, so the publisher can recycle the retired one meanwhile:
    /// the chunk captures its generation on the thread that reads it.
    #[test]
    fn a_later_chunk_waiting_for_a_blocking_thread_holds_no_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let holders = one_blocking_thread_runtime()?
            .block_on(stream_with_a_queued_later_chunk(Duration::ZERO))?;
        assert_eq!(
            holders, 1,
            "only the serving slot may hold the generation while a chunk read waits"
        );
        Ok(())
    }

    /// A chunk's read stage is the storage read alone, without the time the
    /// read waited for a blocking thread.
    #[test]
    fn a_chunk_read_stage_leaves_out_its_wait_for_a_blocking_thread()
    -> Result<(), Box<dyn std::error::Error>> {
        let metrics_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = metrics_recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
        let hold = Duration::from_secs(1);

        one_blocking_thread_runtime()?.block_on(stream_with_a_queued_later_chunk(hold))?;

        let rendered = metrics_handle.render();
        assert_eq!(block_range_stage_samples(&rendered, "read"), Some(3));
        let read_seconds =
            block_range_stage_seconds(&rendered, "read").ok_or("no read stage was recorded")?;
        assert!(
            read_seconds < hold.as_secs_f64(),
            "the read stage counted the {hold:?} its read waited for a blocking thread: {read_seconds}s"
        );
        Ok(())
    }

    /// Each chunk records how long it took to read and to convert, although
    /// both run on the blocking pool.
    ///
    /// Each chunk after the first also records how long its reader waited to
    /// queue it for the client.
    #[tokio::test]
    async fn get_block_range_times_each_chunk_stage() -> Result<(), Box<dyn std::error::Error>> {
        let metrics_recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = metrics_recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&metrics_recorder);
        let (reader, _fixture) = large_block_reader(&large_block_chain(None)?)?;
        let first_chunk_end = u64::from(first_chunk_blocks(&reader)?);
        let adapter = adapter_serving(&reader);

        let blocks =
            range_parity_blocks(&adapter, 1, u64::from(LARGE_BLOCK_TIP), Vec::new()).await?;
        assert_linked_from_genesis(&blocks, LARGE_BLOCK_TIP);
        // The chunk count is the range divided by the blocks one chunk keeps,
        // rounded up.
        let chunks = u64::from(LARGE_BLOCK_TIP).div_ceil(first_chunk_end);
        assert!(chunks >= 3, "the range must span at least three chunks");

        let rendered = metrics_handle.render();
        assert_eq!(block_range_stage_samples(&rendered, "read"), Some(chunks));
        assert_eq!(
            block_range_stage_samples(&rendered, "convert"),
            Some(chunks)
        );
        assert_eq!(
            block_range_stage_samples(&rendered, "send"),
            Some(chunks - 1)
        );
        Ok(())
    }

    /// The first chunk of a range of large blocks ends at its byte cap, and
    /// that edge carries the hash check of every chunk edge.
    ///
    /// A reorg that replaces the chunk's last block ends the stream with
    /// `ABORTED` right after that chunk, and a fork one height above it
    /// continues on the new branch.
    #[tokio::test]
    async fn the_byte_capped_first_chunk_edge_keeps_the_hash_link()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, _fixture) = large_block_reader(&large_block_chain(None)?)?;
        let first_chunk_end = first_chunk_blocks(&reader)?;
        assert!(
            first_chunk_end < LARGE_BLOCK_TIP / 2,
            "the first chunk must stop well inside the range"
        );
        let original_tip = range_parity_blocks(
            &adapter_serving(&reader),
            u64::from(LARGE_BLOCK_TIP),
            u64::from(LARGE_BLOCK_TIP),
            Vec::new(),
        )
        .await?;

        for (fork_height, sent_blocks) in [
            (first_chunk_end, first_chunk_end),
            (first_chunk_end + 1, LARGE_BLOCK_TIP),
        ] {
            let (replacement, _replacement_fixture) =
                large_block_reader(&large_block_chain(Some(fork_height))?)?;
            let slot = CanonicalReaderSlot::new(Arc::clone(&reader));
            let adapter = CompactBlocksGrpcAdapter::new(slot.clone());
            let mut stream = adapter
                .get_block_range(Request::new(block_range(1, LARGE_BLOCK_TIP)))
                .await?
                .into_inner();
            // The test runtime has one thread, so the reader of later chunks
            // has not run yet and reads every later chunk at the replacement.
            drop(slot.publish(replacement));
            let mut blocks = Vec::new();
            let ending = read_blocks(&mut stream, &mut blocks, usize::MAX)
                .await
                .err();
            assert_linked_from_genesis(&blocks, sent_blocks);
            if sent_blocks == LARGE_BLOCK_TIP {
                assert!(ending.is_none(), "{ending:?}");
                assert_ne!(
                    blocks.last().map(|block| &block.hash),
                    original_tip.last().map(|block| &block.hash),
                    "blocks above the fork must come from the new branch"
                );
            } else {
                assert_eq!(
                    ending.map(|status| status.code()),
                    Some(Code::Aborted),
                    "a reorg below the chunk edge must end the stream"
                );
            }
        }
        Ok(())
    }

    /// SHA-256 over the length-delimited protobuf encoding of each streamed
    /// block, in stream order.
    fn range_parity_digest(blocks: &[lightwalletd::CompactBlock]) -> String {
        use sha2::Digest;

        let mut hasher = sha2::Sha256::new();
        for block in blocks {
            hasher.update(block.encode_length_delimited_to_vec());
        }
        hex::encode(hasher.finalize())
    }

    fn transaction_indexes(block: &lightwalletd::CompactBlock) -> Vec<u64> {
        block
            .vtx
            .iter()
            .map(|transaction| transaction.index)
            .collect()
    }

    /// `GetBlockRange` omits a transaction left without data by the requested
    /// pools, as lightwalletd does, and keeps the others at their block
    /// positions.
    #[tokio::test]
    async fn get_block_range_omits_empty_transactions_and_keeps_positions()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = range_parity_adapter()?;

        let shielded = range_parity_blocks(&adapter, 1, 4, Vec::new()).await?;
        let shielded_indexes: Vec<Vec<u64>> = shielded.iter().map(transaction_indexes).collect();
        assert_eq!(
            shielded_indexes,
            vec![vec![1], vec![], vec![1, 3], vec![]],
            "default selection must drop transparent-only transactions",
        );

        let transparent = range_parity_blocks(
            &adapter,
            1,
            4,
            vec![lightwalletd::PoolType::Transparent as i32],
        )
        .await?;
        let transparent_indexes: Vec<Vec<u64>> =
            transparent.iter().map(transaction_indexes).collect();
        assert_eq!(
            transparent_indexes,
            vec![vec![0], vec![0], vec![0, 2], vec![]],
            "transparent selection must drop shielded-only transactions",
        );
        assert!(transparent.iter().all(|block| {
            block
                .vtx
                .iter()
                .all(|transaction| transaction.spends.is_empty() && !transaction.vout.is_empty())
        }));
        Ok(())
    }

    /// Pins the served fixture-range bytes so a serialization drift in
    /// compact-blocks mode fails in CI. `GetBlock` keeps returning the stored
    /// block unpruned.
    #[tokio::test]
    async fn fixture_range_digests_are_pinned() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = range_parity_adapter()?;

        let ascending = range_parity_blocks(&adapter, 1, 4, Vec::new()).await?;
        let descending = range_parity_blocks(&adapter, 4, 1, Vec::new()).await?;
        let transparent = range_parity_blocks(
            &adapter,
            1,
            4,
            vec![lightwalletd::PoolType::Transparent as i32],
        )
        .await?;
        let block = adapter
            .get_block(Request::new(lightwalletd::BlockId {
                height: 3,
                hash: Vec::new(),
            }))
            .await?
            .into_inner();

        let mut reversed = descending.clone();
        reversed.reverse();
        assert_eq!(reversed, ascending, "descending must mirror ascending");

        let observed = [
            (
                "GetBlockRange(1..=4) default pools",
                range_parity_digest(&ascending),
            ),
            (
                "GetBlockRange(4..=1) default pools",
                range_parity_digest(&descending),
            ),
            (
                "GetBlockRange(1..=4) transparent pool",
                range_parity_digest(&transparent),
            ),
            (
                "GetBlock(3)",
                range_parity_digest(std::slice::from_ref(&block)),
            ),
        ];
        let expected = [
            (
                "GetBlockRange(1..=4) default pools",
                "514021d48b7907175a55f0893f937bfe86c8ee7938ded04ac634cd642cc625a0",
            ),
            (
                "GetBlockRange(4..=1) default pools",
                "cf28adb7cc30d9367fb72dfee527becab3721e5dd3df791edad289be258d24f6",
            ),
            (
                "GetBlockRange(1..=4) transparent pool",
                "aab7e6c1a14da6921f1cbdc6d2f7cfc5bdc0ef8b5ba25096c50224abd37cfbf6",
            ),
            (
                "GetBlock(3)",
                "05cf31daef177eb97af4ed6286fea37f325e4b0cbecc6378da5685ebed092b8d",
            ),
        ];
        let drifted: Vec<String> = observed
            .iter()
            .zip(expected)
            .filter(|((_, observed_digest), (_, expected_digest))| {
                observed_digest != expected_digest
            })
            .map(|((label, observed_digest), (_, expected_digest))| {
                format!("{label}: expected {expected_digest}, observed {observed_digest}")
            })
            .collect();
        if drifted.is_empty() {
            Ok(())
        } else {
            Err(drifted.join("\n").into())
        }
    }

    /// Pins every `LightdInfo` field and the `GetLatestBlock` hash order that
    /// the identity contract in `docs/reference/lightwalletd-compatibility.md`
    /// classifies as comparable, live-tip, or Zinder-specific.
    #[tokio::test]
    async fn identity_responses_follow_the_documented_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let tip = canonical.event_fence().visible_tip();
        let nu6_2_branch_id = activations
            .activations()
            .iter()
            .find(|activation| activation.name == "NU6.2")
            .map(|activation| zinder_core::wire::encode_branch_id_hex(activation.branch_id))
            .ok_or("the sample table must include NU6.2")?;
        let tip_block_time = canonical
            .block_header_at(tip.height)?
            .ok_or("the fixture tip must have a header")?
            .block_time;
        let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
            RocksDbCanonicalReader::new(canonical)?,
        )));

        let info = adapter
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?
            .into_inner();
        let latest = adapter
            .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
            .await?
            .into_inner();

        let expected = lightwalletd::LightdInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            vendor: "Zinder".to_owned(),
            taddr_support: false,
            chain_name: "test".to_owned(),
            sapling_activation_height: 1,
            consensus_branch_id: nu6_2_branch_id,
            block_height: 4,
            git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
            branch: String::new(),
            build_date: String::new(),
            build_user: String::new(),
            // Checked against the network estimate below.
            estimated_height: info.estimated_height,
            zcashd_build: String::new(),
            zcashd_subversion: String::new(),
            donation_address: String::new(),
            upgrade_name: "NU6.3".to_owned(),
            upgrade_height: 603,
            lightwallet_protocol_version:
                zinder_proto::compat::lightwalletd::LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
        };
        assert_eq!(info, expected);
        // The fixture tip is old, so the network estimate runs ahead of it,
        // as Zebra's estimate from the same tip would.
        let estimate = zinder_compat_lightwalletd::estimated_network_height(
            &activations,
            tip.height,
            tip_block_time,
            unix_now()?,
        );
        assert!(
            info.estimated_height.abs_diff(u64::from(estimate.value())) <= 1,
            "estimated {} where {} was expected",
            info.estimated_height,
            estimate.value()
        );
        assert!(info.estimated_height > info.block_height);
        assert_eq!(latest.height, info.block_height);
        assert_eq!(
            latest.hash,
            zinder_core::wire::encode_internal_block_hash(tip.hash).to_vec()
        );
        assert_eq!(latest.hash.len(), 32);
        Ok(())
    }

    fn unix_now() -> Result<i64, Box<dyn std::error::Error>> {
        Ok(i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        )?)
    }

    /// Tip calls refuse a stale tip the writer's node does not report.
    ///
    /// A served tip older than the stale-tip limit fails `GetLatestBlock`
    /// and `GetLightdInfo` with `UNAVAILABLE` unless the writer's node
    /// reports that tip, while block ranges keep serving. A disabled limit
    /// serves the old tip.
    #[tokio::test]
    async fn tip_calls_refuse_a_stale_tip_unless_the_limit_is_disabled()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let (reader, _fixture) = large_block_reader(&chain)?;
        // The fixture's blocks are months old.
        let limited = adapter_serving(&reader).with_stale_tip_limit(
            zinder_compat_lightwalletd::StaleTipLimit::after(Duration::from_mins(25)),
        );
        let latest = limited
            .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
            .await
            .err()
            .map(|status| status.code());
        let info = limited
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await
            .err()
            .map(|status| status.code());
        assert_eq!(latest, Some(Code::Unavailable));
        assert_eq!(info, Some(Code::Unavailable));
        let blocks = range_parity_blocks(&limited, 1, 4, Vec::new()).await?;
        assert_eq!(blocks.len(), 4);

        // The writer's node reports the served tip: the chain is only quiet.
        let node_tip_witness = zinder_compat_lightwalletd::NodeTipWitness::default();
        let vouched = adapter_serving(&reader)
            .with_stale_tip_limit(zinder_compat_lightwalletd::StaleTipLimit::after(
                Duration::from_mins(25),
            ))
            .with_node_tip_witness(node_tip_witness.clone());
        node_tip_witness.observe(Some(BlockHeight::new(4)));
        vouched
            .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
            .await?;
        vouched
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?;
        // The writer is behind a node that has a newer block.
        let behind_witness = zinder_compat_lightwalletd::NodeTipWitness::default();
        let behind = adapter_serving(&reader)
            .with_stale_tip_limit(zinder_compat_lightwalletd::StaleTipLimit::after(
                Duration::from_mins(25),
            ))
            .with_node_tip_witness(behind_witness.clone());
        behind_witness.observe(Some(BlockHeight::new(9)));
        assert_eq!(
            behind
                .get_lightd_info(Request::new(lightwalletd::Empty::default()))
                .await
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable)
        );
        // The node has been unreachable since the witness started.
        let unreachable_witness = zinder_compat_lightwalletd::NodeTipWitness::default();
        let unreachable = adapter_serving(&reader)
            .with_stale_tip_limit(zinder_compat_lightwalletd::StaleTipLimit::after(
                Duration::from_mins(25),
            ))
            .with_node_tip_witness(unreachable_witness.clone());
        unreachable_witness.observe(None);
        assert_eq!(
            unreachable
                .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
                .await
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable)
        );

        let unlimited = adapter_serving(&reader)
            .with_stale_tip_limit(zinder_compat_lightwalletd::StaleTipLimit::DISABLED);
        unlimited
            .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
            .await?;
        unlimited
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "The fixture contract test keeps the supported and explicitly rejected RPC surface auditable together."
    )]
    async fn fixture_adapter_serves_four_methods_and_rejects_the_rest()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let expected_tip = canonical.event_fence().visible_tip();
        let expected_block = canonical
            .compact_block_at(BlockHeight::new(2))?
            .ok_or("fixture block missing")?;
        let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
            RocksDbCanonicalReader::new(canonical)?,
        )));

        let info = adapter
            .get_lightd_info(Request::new(lightwalletd::Empty::default()))
            .await?
            .into_inner();
        assert!(!info.taddr_support);
        assert_eq!(info.block_height, u64::from(expected_tip.height.value()));

        let latest = adapter
            .get_latest_block(Request::new(lightwalletd::ChainSpec::default()))
            .await?
            .into_inner();
        assert_eq!(latest.height, u64::from(expected_tip.height.value()));

        let block = adapter
            .get_block(Request::new(lightwalletd::BlockId {
                height: 2,
                hash: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(block.height, u64::from(expected_block.height().value()));
        assert_eq!(
            block.hash,
            zinder_core::wire::encode_internal_block_hash(expected_block.block_hash()).to_vec()
        );
        assert_eq!(
            block.prev_hash,
            zinder_core::wire::encode_internal_block_hash(expected_block.previous_block_hash())
                .to_vec()
        );
        assert_eq!(block.vtx.len(), expected_block.transactions().len());

        let transparent = adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: 2,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: 2,
                    hash: Vec::new(),
                }),
                pool_types: vec![lightwalletd::PoolType::Transparent as i32],
            }))
            .await?
            .into_inner()
            .next()
            .await
            .ok_or("transparent range returned no block")??;
        assert!(transparent.vtx.iter().all(|transaction| {
            transaction.spends.is_empty()
                && transaction.outputs.is_empty()
                && transaction.actions.is_empty()
                && transaction.ironwood_actions.is_empty()
        }));

        let mut stream = adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: 4,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: 1,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            }))
            .await?
            .into_inner();
        let mut heights = Vec::new();
        while let Some(block) = stream.next().await {
            heights.push(block?.height);
        }
        assert_eq!(heights, vec![4, 3, 2, 1]);

        assert_unimplemented(
            adapter
                .get_block_nullifiers(Request::new(lightwalletd::BlockId::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_block_range_nullifiers(Request::new(lightwalletd::BlockRange::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_transaction(Request::new(lightwalletd::TxFilter::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .send_transaction(Request::new(lightwalletd::RawTransaction::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_taddress_balance(Request::new(lightwalletd::AddressList::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_taddress_txids(Request::new(
                    lightwalletd::TransparentAddressBlockFilter::default(),
                ))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_taddress_transactions(Request::new(
                    lightwalletd::TransparentAddressBlockFilter::default(),
                ))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_mempool_stream(Request::new(lightwalletd::Empty::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_latest_tree_state(Request::new(lightwalletd::Empty::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_tree_state(Request::new(lightwalletd::BlockId::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg::default()))
                .await,
        );
        assert_unimplemented(
            adapter
                .get_address_utxos_stream(Request::new(lightwalletd::GetAddressUtxosArg::default()))
                .await,
        );
        let ping_status = adapter
            .ping(Request::new(lightwalletd::Duration::default()))
            .await
            .err()
            .map(|status| status.code());
        assert_eq!(ping_status, Some(Code::FailedPrecondition));
        Ok(())
    }

    #[tokio::test]
    async fn generated_client_rejects_client_streaming_rpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(4);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
            RocksDbCanonicalReader::new(canonical)?,
        )));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(compact_tx_streamer_server::CompactTxStreamerServer::new(
                    adapter,
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await
        });

        let mut client = CompactTxStreamerClient::connect(endpoint).await?;
        let error = client
            .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address::default()]))
            .await
            .err()
            .ok_or("client-streaming RPC unexpectedly succeeded")?;
        assert_eq!(error.code(), Code::Unimplemented);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn served_block_ranges_refuse_a_pool_type_flood_before_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(2);
        let activations = Arc::new(zinder_testkit::sample_regtest_upgrade_activations());
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, activations.as_ref())?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let adapter = CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
            RocksDbCanonicalReader::new(canonical)?,
        )));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(adapter.into_server())
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await
        });
        let block_range = |pool_types| lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 2,
                hash: Vec::new(),
            }),
            pool_types,
        };
        let sapling = lightwalletd::PoolType::Sapling as i32;

        let mut client = CompactTxStreamerClient::connect(endpoint).await?;
        let refusal = client
            .get_block_range(block_range(vec![
                sapling;
                zinder_runtime::MAX_DECODING_MESSAGE_BYTES
                    - 64
            ]))
            .await
            .err()
            .ok_or("a pool-type flood was accepted")?;
        let mut blocks = client
            .get_block_range(block_range(vec![sapling]))
            .await?
            .into_inner();
        let mut streamed_blocks = 0;
        while blocks.message().await?.is_some() {
            streamed_blocks += 1;
        }

        assert_eq!(refusal.code(), Code::InvalidArgument);
        assert!(
            refusal
                .message()
                .starts_with("GetBlockRange request field poolTypes carries "),
            "{refusal:?}"
        );
        assert_eq!(streamed_blocks, 2);
        let _ = shutdown_sender.send(());
        server.await??;
        Ok(())
    }

    /// The compact-blocks listener's health follows `/readyz`.
    ///
    /// The listener is assembled and supervised as the runtime assembles and
    /// supervises it. The test publishes the shutdown itself first, so it
    /// can check health while the listener still answers.
    #[tokio::test]
    async fn compact_blocks_listener_health_follows_readyz_through_readiness_and_shutdown()
    -> eyre::Result<()> {
        use tonic_health::pb::health_check_response::ServingStatus;
        use zinder_testkit::{PublicListenerProbe, next_watch_status, watch_ends};

        let chain =
            zinder_testkit::ChainFixture::new(zinder_core::Network::ZcashRegtest).extend_blocks(1);
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let mut fixture =
            zinder_testkit::WalletServingStoreFixture::from_chain(&chain, &activations)?;
        let (canonical, wallet) = fixture.take_readers()?;
        drop(wallet);
        let readiness = Readiness::default();
        let serving_readiness = CompactServingReadiness::new(readiness.clone());
        let runtime = zinder_runtime::SupervisedRuntime::new(
            zinder_runtime::RuntimeService::CompatLightwalletd,
        );
        let router = runtime.public_grpc_router(
            CompactBlocksGrpcAdapter::new(CanonicalReaderSlot::new(Arc::new(
                RocksDbCanonicalReader::new(canonical)?,
            )))
            .into_server(),
            zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
            zinder_runtime::ClientAddressSource::Connection,
            &serving_readiness.runtime(),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listener_address = listener.local_addr()?;
        let server = router.serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            runtime.grpc_server_shutdown(),
        );
        let shutdown = runtime.shutdown_request();
        let supervised = tokio::spawn({
            let serving_readiness = serving_readiness.clone();
            runtime.supervise(server, zinder_runtime::SupervisedTasks::new(), move || {
                serving_readiness.publish_shutting_down();
            })
        });
        let (ops_address, _readyz) = spawn_readyz(&readiness).await?;
        let probe = PublicListenerProbe::connect(listener_address, ops_address).await?;
        let mut streamer = CompactTxStreamerClient::new(probe.channel());
        let mut watch = probe.watch("").await?;

        assert_eq!(probe.agreed_status().await?, ServingStatus::NotServing);
        assert_eq!(
            next_watch_status(&mut watch).await?,
            ServingStatus::NotServing
        );
        let refused = streamer.get_latest_block(lightwalletd::ChainSpec {}).await;
        assert_eq!(
            refused.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        let reflected = probe.reflected_services().await?;
        assert!(
            reflected
                .iter()
                .any(|name| name == "cash.z.wallet.sdk.rpc.CompactTxStreamer")
                && reflected.iter().any(|name| name == "grpc.health.v1.Health"),
            "reflection did not describe the streamer and health services: {reflected:?}"
        );

        serving_readiness.publish_storage(ReadinessState::ready(Some(1)));
        assert_eq!(probe.agreed_status().await?, ServingStatus::Serving);
        assert_eq!(next_watch_status(&mut watch).await?, ServingStatus::Serving);
        streamer
            .get_latest_block(lightwalletd::ChainSpec {})
            .await?;

        serving_readiness.publish_shutting_down();
        assert_eq!(probe.agreed_status().await?, ServingStatus::NotServing);
        assert_eq!(
            next_watch_status(&mut watch).await?,
            ServingStatus::NotServing
        );

        shutdown.request();
        watch_ends(&mut watch).await?;
        supervised.await??;
        Ok(())
    }

    async fn spawn_readyz(
        readiness: &Readiness,
    ) -> eyre::Result<(std::net::SocketAddr, zinder_runtime::OpsEndpointHandle)> {
        let ops_reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let ops_address = ops_reservation.local_addr()?;
        drop(ops_reservation);
        let handle = zinder_runtime::spawn_ops_endpoint(
            ops_address,
            zinder_runtime::OpsServer {
                service_name: "zinder-compat-lightwalletd",
                service_version: "0.0.0",
                network_name: "zcash-regtest",
                advertised_capabilities: Arc::from([]),
            },
            readiness.clone(),
        )
        .await?;
        Ok((ops_address, handle))
    }
}

#[cfg(test)]
mod teardown_tests {
    use super::{CompactBlockServingError, tear_down_on_blocking_thread};

    /// Stands in for a publisher whose secondary panics while closing.
    struct PanicsWhenDropped;

    impl Drop for PanicsWhenDropped {
        #[allow(clippy::panic, reason = "the test needs a teardown that panics")]
        fn drop(&mut self) {
            panic!("closing a secondary panicked");
        }
    }

    #[tokio::test]
    async fn a_teardown_that_panics_is_an_error() {
        let teardown = tear_down_on_blocking_thread(PanicsWhenDropped).await;

        assert!(
            matches!(teardown, Err(CompactBlockServingError::Teardown(_))),
            "a panicking teardown must fail the publisher task, got {teardown:?}"
        );
    }

    #[tokio::test]
    async fn a_clean_teardown_succeeds() -> Result<(), CompactBlockServingError> {
        tear_down_on_blocking_thread(()).await
    }
}
